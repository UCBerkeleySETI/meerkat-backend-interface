# -*- coding: future_fstrings -*-

"""Module to interface with Telstate to retrieve calibration solutions.

   See also:
       scripts/applied_phases.py

   and the following external scripts:
       fbfuse_phaseup.py (Ludwig Schwardt, SARAO)
       bluse_telstate.py

   Some of the Telstate-specific functions in this module are based on 
   those in the scripts listed above (especially fbfuse_phaseup.py and
   bluse_telstate.py)
"""
import numpy as np
import warnings
import redis
import logging
import os
import time
import functools
import pathlib
from datetime import datetime

import katsdptelstate

from meerkat_backend_interface.logger import log, set_logger

class TelstateInterface(object):
    """This class is used to interface with Telstate to retrieve calibration
       information from MeerKAT.
    """

    def __init__(self):
        """Initialise the interface and logging. 
        
           Args:
               local_redis (str): Local Redis endpoint (host:port)
        """
        log = set_logger(log_level = logging.DEBUG)
 
    def query_telstate(self, telstate_redis, output_path):
        """Query the current Telstate Redis DB for the latest calibration 
           
           Args:
               telstate_redis (str): Redis endpoint for Telstate (host:port)
               output_path (str): Output file path for saving cal data. 
        """
        log.info('Querying Telstate at {}'.format(telstate_redis))
        # Create TelescopeState object for current subarray:
        telstate = katsdptelstate.TelescopeState(telstate_redis)
        # Query most recent phaseup time from Telstate:
        phaseup_time = self.get_phaseup_time(telstate)
        # Get calibration solutions
        # Default for average gain (100.0)
        # Default for flatten bandpass (True)
        corrections, cal_G, cal_B, cal_K = self.get_phaseup_corrections(telstate, 
                                           phaseup_time,
                                           100.0,
                                           True)

        # Time of retrieval:
        r_time = datetime.utcnow()
        r_time = r_time.strftime("%Y%m%dT%H%M%S")
 
        # Save .npz file for diagnostic purposes.
        output_file = os.path.join(output_path, 'cal_solutions_{}'.format(r_time))
        log.info('Saving cal solutions to {}'.format(output_file))
        np.savez(output_file, cal_G=cal_G, cal_B=cal_B, cal_K=cal_K, 
            cal_all=corrections)

        return cal_K, cal_G, cal_B, corrections, r_time

    def get_phaseup_corrections(self, telstate, end_time, target_average_correction,
                                flatten_bandpass):
        """Get corrections associated with phase-up that ended at `end_time`.

        Parameters
        ----------
        telstate : :class:`katsdptelstate.TelescopeState`
            Top-level telstate object
        end_time : float
            Time when phase-up successfully completed, as Unix timestamp
        target_average_correction : float
            The global average F-engine gain for all inputs to ensure good quantisation
        flatten_bandpass : bool
            True to flatten the shape of the bandpass magnitude (i.e. do different
            per-channel amplitude gains)

        Returns
        -------
        corrections : dict mapping string to array of complex
            Complex F-engine gains per channel per input that will phase up the
            inputs and optionally correct their bandpass shapes
        """
        # Obtain capture block ID associated with successful phase-up
        phaseup_cbid = telstate.get_range('sdp_capture_block_id', et=end_time)
        if not phaseup_cbid:
            # The phase-up probably happened in an earlier subarray, so report it
            all_cbids = telstate.get_range('sdp_capture_block_id', st=0)
            if not all_cbids:
                raise CalSolutionsUnavailable('Subarray has not captured any data yet')
            first_cbid, start_time = all_cbids[0]
            raise CalSolutionsUnavailable('Requested phase-up time is {} but current '
                                          'subarray only started capturing data at {} (cbid {})'
                                          .format(end_time, start_time, first_cbid))
        cbid, start_time = phaseup_cbid[0]
        view = self._telstate_capture_stream(telstate, cbid, 'cal')
        _get = functools.partial(self.get_cal_solutions, view,
                                 start_time=start_time, end_time=end_time)
        # Wait for the last relevant bfcal product from the pipeline
        try:
            hv_gains = _get('BCROSS_DIODE_SKY')
        except CalSolutionsUnavailable as err:
            log.warning("No BCROSS_DIODE_SKY solutions found - "
                           "falling back to BCROSS_DIODE only: %s", err)
            hv_gains = _get('BCROSS_DIODE')
        hv_delays = _get('KCROSS_DIODE')
        gains = _get('G')
        bp_gains = _get('B')
        delays = _get('K')
        # Add HV delay to the usual delay
        for inp in sorted(delays):
            delays[inp] += hv_delays.get(inp, 0.0)
            if np.isnan(delays[inp]):
                log.warning("Delay fit failed on input %s (all its "
                               "data probably flagged)", inp)
        # Add HV phase to bandpass phase
        for inp in bp_gains:
            bp_gains[inp] *= hv_gains.get(inp, 1.0)
        bp_gains = self.clean_bandpass(bp_gains)
        cal_channel_freqs = self.get_cal_channel_freqs(view)
        return self.calculate_corrections(gains, bp_gains, delays, 
                                     cal_channel_freqs,
                                     target_average_correction, 
                                     flatten_bandpass), gains, bp_gains, delays

    def get_cal_inputs(self, view):
        """Get list of input labels associated with calibration products.
           From fbfuse_telstate.py and bluse_telstate.py 
        """
        try:
            ants = view['antlist']
            pols = view['pol_ordering']
        except KeyError as err:
            raise CalSolutionsUnavailable(str(err)) from err
        return [ant + pol for pol in pols for ant in ants]

    def get_cal_channel_freqs(self, view):
        """Get sky frequencies (in Hz) associated with bandpass cal solutions.
           From fbfuse_telstate.py and bluse_telstate.py 
        """
        try:
            bandwidth = view['bandwidth']
            center_freq = view['center_freq']
            n_chans = view['n_chans']
        except KeyError as err:
            raise CalSolutionsUnavailable(str(err)) from err
        return center_freq + (np.arange(n_chans) - n_chans / 2) * (bandwidth / n_chans)

    def _telstate_capture_stream(self, telstate, capture_block_id, stream_name):
        """Create telstate having only <stream> and <cbid_stream> views.
           From fbfuse_telstate.py and bluse_telstate.py 
        """
        capture_stream = telstate.join(capture_block_id, stream_name)
        return telstate.view(stream_name, exclusive=True).view(capture_stream)

    def clean_bandpass(self, bp_gains):
        """Clean up bandpass gains by linear interpolation across flagged regions."""
        print("=== in clean_bandpass() ===")
        clean_gains = {}
        # Linearly interpolate across flagged regions
        for inp, bp in bp_gains.items():
            flagged = np.isnan(bp)
            if flagged.all():
                print(f"{inp} all flagged")
                clean_gains[inp] = bp
                continue
            #print(f"{inp} NOT all flagged")
            chans = np.arange(len(bp))
            clean_gains[inp] = np.interp(chans, chans[~flagged], bp[~flagged])
        return clean_gains

    def get_cal_solutions(self, view, name, timeout=0., start_time=None, end_time=None):
        """Retrieve calibration solutions from telescope state.

        Parameters
        ----------
        view : :class:`katsdptelstate.TelescopeState`
            Telstate with the appropriate view of calibration products
        name : string
            Identifier of desired calibration solutions (e.g. 'K', 'G', 'B')
        timeout : float, optional
            Time to wait for solutions to appear, in seconds
        start_time : float, optional
            Look for solutions based on data captured after this time
            (defaults to the start of time, i.e. the birth of the subarray)
        end_time : float, optional
            Look for solutions based on data captured before this time
            (defaults to the end of time)

        Returns
        -------
        solutions : dict mapping string to float / array
            Calibration solutions associated with each correlator input

        Raises
        ------
        CalSolutionsUnavailable
            If the requested cal solutions are not available for any reason


        From fbfuse_telstate.py and bluse_telstate.py 
        """
        if start_time is None:
            start_time = 0.0
        # Check early whether the cal pipeline is even running
        inputs = self.get_cal_inputs(view)
        key = 'product_' + name
        try:
            # Bandpass-like cal is special as it has multiple parts (split cal)
            n_parts = view['product_B_parts'] if name.startswith('B') else 0
            solutions = self._get_latest_within_interval(
                view, key, timeout, start_time, end_time, n_parts)
        except (KeyError, katsdptelstate.TimeoutError, ValueError) as err:
            msg = 'No {} calibration solutions found: {}'.format(name, err)
            raise CalSolutionsUnavailable(msg)
        log.info('Found %s solutions', name)
        # The sign of katsdpcal delays are opposite to that of corr delay model
        if name.startswith('K'):
            solutions = -solutions
        # The HV delay / phase is a single number per channel per polarisation
        # for the entire array, but the solver gets estimates per antenna.
        # The cal pipeline has standardised on using the nanmedian solution
        # instead of picking the solution of the reference antenna.
        # Copy this solution for all antennas to keep the shape of the array.
        if name[1:].startswith('CROSS_DIODE'):
            with warnings.catch_warnings():
                # All antennas could have NaNs in one channel so don't warn
                warnings.filterwarnings('ignore', r'All-NaN slice encountered')
                solutions[:] = np.nanmedian(solutions, axis=-1, keepdims=True)
        # Collapse the polarisation and antenna axes into one input axis at end
        solutions = solutions.reshape(solutions.shape[:-2] + (-1,))
        # Move input axis to front and pair up with input labels to form dict
        return dict(zip(inputs, np.moveaxis(solutions, -1, 0)))

    def _get_latest_within_interval(self, view, key, timeout, start_time, end_time,
                                    n_parts=0):
        """Get latest value of `key` from telstate `view` within time interval.

        The interval is given by [`start_time`, `end_time`). If `end_time`
        is None the interval becomes open-ended: [`start_time`, inf). In that
        case, wait for up to `timeout` seconds for a value to appear. Raise a
        :exc:`katsdptelstate.TimeoutError` or :exc:`KeyError` if no values were
        found in the interval.

        If `n_parts` is a positive integer, the sensor is array-valued and
        split across that many parts, which are indexed by appending a sequence
        of integers to `key` to obtain the actual telstate keys. The values of
        the key parts will be stitched together along the first dimension of
        each array. If only some produce values within the timeout, the missing
        parts are replaced with arrays of NaNs.

        From fbfuse_telstate.py and bluse_telstate.py 
        """
        # Coerce n_parts to int to catch errors early (it comes from telstate)
        n_parts = int(n_parts)
        # Handle the simple non-split case first
        if n_parts <= 0:
            if end_time is None:
                # Wait for fresh value to appear
                fresh = lambda value, ts: ts >= start_time  # noqa: E731
                view.wait_key(key, fresh, timeout)
                return view[key]
            else:
                # Assume any value in the given interval would already be there
                solution_before_end = view.get_range(key, et=end_time)
                if solution_before_end and solution_before_end[0][1] >= start_time:
                    return solution_before_end[0][0]
                else:
                    raise KeyError('No {} found between timestamps {} and {}'
                                   .format(key, start_time, end_time))
        # Handle the split case (n_parts is now a positive integer)
        parts = []
        valid_part = None
        deadline = time.time() + timeout
        for i in range(n_parts):
            timeout_left = max(0.0, deadline - time.time())
            try:
                valid_part = self._get_latest_within_interval(
                    view, key + str(i), timeout_left, start_time, end_time)
            except (KeyError, katsdptelstate.TimeoutError) as err:
                if end_time is None:
                    # Don't use err's msg as that will give `timeout_left` secs
                    log.warning('Timed out after %g seconds waiting '
                                   'for telstate keys %s*', timeout, key)
                else:
                    log.warning(str(err))
                parts.append(None)
            else:
                parts.append(valid_part)
        if valid_part is None:
            raise KeyError('All {}* keys either timed out or were not found '
                           'within interval'.format(key))
        # If some (but not all) of the solution was missing, fill it with NaNs
        for i in range(n_parts):
            if parts[i] is None:
                parts[i] = np.full_like(valid_part, np.nan)
        return np.concatenate(parts)

    def calculate_corrections(self, G_gains, B_gains, delays, cal_channel_freqs,
                              target_average_correction, flatten_bandpass,
                              random_phase=False):
        """Turn cal pipeline products into corrections to be passed to F-engine.
           From fbfuse_telstate.py and bluse_telstate.py 
        """
        average_gain = {}
        gain_corrections = {}
        # First find relative corrections per input with arbitrary global average
        for inp in G_gains:
            # Combine all calibration products for input into single array of gains
            K_gains = np.exp(-2j * np.pi * delays[inp] * cal_channel_freqs)
            gains = K_gains * B_gains[inp] * G_gains[inp]
            if np.isnan(gains).all():
                average_gain[inp] = gain_corrections[inp] = 0.0
                continue
            abs_gains = np.abs(gains)
            # Track the average gain to fix overall power level (and as diagnostic)
            average_gain[inp] = np.nanmedian(abs_gains)
            corrections = 1.0 / gains
            if not flatten_bandpass:
                # Let corrections have constant magnitude equal to 1 / (avg gain),
                # which ensures that power levels are still equalised between inputs
                corrections *= abs_gains / average_gain[inp]
            if random_phase:
                corrections *= np.exp(2j * np.pi * np.random.rand(len(corrections)))
            gain_corrections[inp] = np.nan_to_num(corrections)
        # All invalid gains (NaNs) have now been turned into zeros
        valid_average_gains = [g for g in average_gain.values() if g > 0]
        if not valid_average_gains:
            raise ValueError("All gains invalid and beamformer output will be zero!")
        global_average_gain = np.median(valid_average_gains)
        # Iterate over inputs again and fix average values of corrections
        for inp in sorted(G_gains):
            relative_gain = average_gain[inp] / global_average_gain
            if relative_gain == 0.0:
                log.warning("%s has no valid gains and will be zeroed", inp)
                continue
            # This ensures that input at the global average gets target correction
            gain_corrections[inp] *= target_average_correction * global_average_gain
            safe_relative_gain = np.clip(relative_gain, 0.5, 2.0)
            if relative_gain == safe_relative_gain:
                #log.info("%s: average gain relative to global average = %5.2f",
                #            inp, relative_gain)
                pass
            else:
                log.warning("%s: average gain relative to global average "
                               "= %5.2f out of range, clipped to %.1f",
                               inp, relative_gain, safe_relative_gain)
                gain_corrections[inp] *= relative_gain / safe_relative_gain
        return gain_corrections

    def get_phaseup_time(self, telstate):
        """Timestamp of last successful phaseup (or 0 if none found).
           From fbfuse_telstate.py and bluse_telstate.py 
        """
        end_time = 0.
        cbids = list(zip(*telstate.get_range('sdp_capture_block_id', st=0)))[0]
        for cbid in cbids:
            view = telstate.view(cbid, exclusive=True)
            if 'obs_params' not in view:
                print(f"no obs_params for {cbid}")
                continue
            if 'cal_product_G' not in view:
                print(f"no cal_product_G for {cbid}")
                continue
            #if b'script_name' not in view['obs_params'].keys():
            #    print(f"no script_name for {cbid} obs_param.keys()")
            #    print(view['obs_params'].keys())
            #    continue
            if b'script_name' not in view['obs_params']:
                print(f"no script_name for {cbid} obs_param")
                continue
            script = view['obs_params'][b'script_name']
            if type(script) == bytes:
                script = script.decode()
            script = pathlib.Path(script).name
            # Only consider these scripts (this may be expanded in future)
            if script in ("calibrate_delays.py", "bf_phaseup.py"):
                #Focus on the G solutions since they are last in the line of K,B,G
                gain_times = list(zip(*view.get_range('cal_product_G', st=0)))[1]
                # Only consider cases with two solution intervals successful
                if len(gain_times) == 2:
                    # Go a little bit beyond the second ("corrected") interval
                    end_time = gain_times[1] + 10.
        return end_time

class CalSolutionsUnavailable(Exception):
    """Requested calibration solutions are not available from cal pipeline.
       From fbfuse_telstate.py and bluse_telstate.py 
    """
