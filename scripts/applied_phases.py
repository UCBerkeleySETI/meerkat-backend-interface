"""Script to retrieve applied phase solutions from KATPortal. 
   Note that these phase solutions are only useful if they have been 
   applied by the primary observer (ie the F-engine outputs have been 
   phased to boresight). 

   Needs to be provided with an antenna-based sensor with braces in place of 
   antenna name.

   The proxy name must be included (which means also specifying the subarray 
   index). 

   e.g. 
   cbf_{}_wide_antenna_channelised_voltage_{}h_eq

   In addition, the number of the current active subarray must be specified. 
   In this way, any per-antenna sensor can be retrieved with this script.    
 
   The output is saved as a serialised dictionary (using Python Pickle). 
"""

from tornado import ioloop
from tornado.gen import coroutine
from katportalclient import KATPortalClient
import redis
import csv
import logging
import argparse
import sys
import pickle
import ast
import subprocess
import redis
import time
from datetime import datetime
import numpy as np
import os

def cli(args = sys.argv[0]):
    """CLI for antenna sensor retrieval.
    """
    usage = "{} [options]".format(args)
    description = 'Retrieve sensor values for a specified sensor for each antenna in an active subarray.'
    parser = argparse.ArgumentParser(prog = 'applied_phases', usage = usage, 
        description = description)
    parser.add_argument('--pattern', 
        type = str,
        help = 'Sensor name. Replace subarray number and antenna name with braces: {}')
    parser.add_argument('--subarray', 
        type = str,
        help = 'Active subarray number.') 
    parser.add_argument('--outfile', 
        type = str,
        help = 'Output file name.') 
    if(len(sys.argv[1:])==0):
        parser.print_help()
        parser.exit()
    args = parser.parse_args()
    main(sensor_pattern = args.pattern, subarray_number = args.subarray, outfile = args.outfile)

@coroutine
def fetch_sensor_pattern(pattern, client, log):
    """Fetch sensor pattern for each antenna. 
       
       Args:
           pattern (str): per-antenna sensor name with antenna fields
                          replaced with braces.
           client (obj): KATPortalClient object.
           log: logger

       Returns:
           sensor_details (dict): sensor results including value and timestamp
                                  of last change in value.
           None if no sensor results obtainable. 
    """
    try:
        sensor_details = yield client.sensor_values(pattern, include_value_ts=True)
        return(sensor_details)
    except Exception as e:
        log.error(e)
        return(None)

@coroutine
def fetch_sensor_names(pattern, client, log):
    """Fetch matching sensor names for a specified pattern. 
       
       Args:
           pattern (str): pattern to match.
           client (obj): KATPortalClient object.
           log: logger

       Returns:
           sensor_names (dict): sensor names matching supplied pattern.
           None if no sensor results obtainable. 
    """
    log.info("Checking for sensor pattern: {}".format(pattern))
    try:
        sensor_names = yield client.sensor_names(pattern)
        #if not sensor_names:
        #    log.warning("No matching sensors found for {}".format(pattern))
        #    return(None)
        #else:
        log.info("Match for telstate endpoint sensor: {}".format(sensor_names))
        #    return(sensor_names)
    except Exception as e:
        log.error(e)
        return(None)

def main(sensor_pattern, subarray_number, outfile):
    """Retrieves values for a specific sensor from each antenna in an 
       active subarray.
       
       Args:
           pattern (str): per-antenna sensor name with antenna fields
                          replaced with braces.
           subarray_number (int): number of the current active subarray. 
           outfile (str): filename for output .pkl file. 

       Returns:
           None       
    """
    LOGGING_FORMAT = "[%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s] %(message)s"
    logging.basicConfig(format=LOGGING_FORMAT)
    log = logging.getLogger('BLUSE')
    log.setLevel(logging.DEBUG)
    log.info("Starting calibration solution retrieval script...")

    # Retrieve CAM address for current subarray:    
    subarray_name = 'array_{}'.format(subarray_number)
    redis_server = redis.StrictRedis(decode_responses = True) 
    cam_url = redis_server.get("{}:{}".format(subarray_name, 'cam:url'))

    # Instantiate client for retrieval of sensor data from CAM:
    client = KATPortalClient(cam_url, on_update_callback=None, logger=log)
    io_loop = ioloop.IOLoop.current()

    # Check last delaycal
    delaycal_sensor = 'subarray_{}_script_last_delay_calibration'.format(subarray_number) 
    delaycal_details = io_loop.run_sync(lambda: fetch_sensor_pattern(delaycal_sensor, client, log))
    for sensor, details in delaycal_details.items():
        last_delaycal = details.value
        if(last_delaycal):
            delaycal_ts = details.value_time 
            log.info('Last delaycal: Schedule Block {} at {}'.format(last_delaycal, delaycal_ts))
        else:
            delaycal_ts = None
            log.info('No delaycal')

    # Check last phaseup
    phaseup_sensor = 'subarray_{}_script_last_phaseup'.format(subarray_number) 
    phaseup_details = io_loop.run_sync(lambda: fetch_sensor_pattern(phaseup_sensor, client, log))
    for sensor, details in phaseup_details.items():
        last_phaseup = details.value
        if(last_phaseup): 
            phaseup_ts = details.value_time 
            log.info('Last phaseup: {} at {}'.format(last_phaseup, phaseup_ts))
        else:
            phaseup_ts = None
            log.info('No phaseup')

    # Attempting sensor name lookup:
    # Since the full name of the telstate sensor changes based on subarray characteristics, 
    # we attempt to look up the closest match:
    # Note: Appears sensor lookup doesn't work with product IDs 
    telstate_sensor_pattern = 'sdp_*.spmc_array_*_*_*_telstate.telstate'
    telstate_details = io_loop.run_sync(lambda: fetch_sensor_names(telstate_sensor_pattern, client, log))
    telstate_sensor_pattern = 'sdp_*.spmc_*_telstate.telstate'
    telstate_details = io_loop.run_sync(lambda: fetch_sensor_names(telstate_sensor_pattern, client, log))
    telstate_sensor_pattern = 'sdp_1.spmc_array_1_wide_2_telstate.telstate'
    telstate_details = io_loop.run_sync(lambda: fetch_sensor_names(telstate_sensor_pattern, client, log))
    telstate_sensor_pattern = 'sdp_1.spmc_array_1_*_telstate.telstate'
    telstate_details = io_loop.run_sync(lambda: fetch_sensor_names(telstate_sensor_pattern, client, log))
    telstate_sensor_pattern = 'sdp_*.spmc_array_1*_telstate.telstate'
    telstate_details = io_loop.run_sync(lambda: fetch_sensor_names(telstate_sensor_pattern, client, log))
    telstate_sensor_pattern = 'sdp_1.*_telstate.telstate'
    telstate_details = io_loop.run_sync(lambda: fetch_sensor_names(telstate_sensor_pattern, client, log))

    # Find sdp product_id:
    sdp_id_sensor = 'sdp_{}_subarray_product_ids'.format(subarray_number)
    sdp_id_details = io_loop.run_sync(lambda: fetch_sensor_pattern(sdp_id_sensor, client, log))
    if(sdp_id_details is not None): # Check, since this sensor disappears when not active it seems
        for sensor, details in sdp_id_details.items():
            sdp_id = details.value

    # Build telstate sensor name:
    telstate_sensor = 'sdp_{}_spmc_{}_telstate_telstate'.format(subarray_number, sdp_id)

    # Provide telstate connection information
    log.info('Fetching telstate endpoint via sensor: {}'.format(telstate_sensor))
    telstate_details = io_loop.run_sync(lambda: fetch_sensor_pattern(telstate_sensor, client, log))
    if(telstate_details is not None): # Check, since this sensor disappears when not active it seems
        for sensor, details in telstate_details.items():
            telstate_endpoint = ast.literal_eval(details.value)
        endpoint_ip = telstate_endpoint[0]
        endpoint_port = telstate_endpoint[1] 
        log.info('Telstate endpoint IP address: {} and port: {}'.format(endpoint_ip, endpoint_port))
        # Fetch and save phase solutions
        script_env = '/opt/virtualenv/bluse3/bin/python3.5'
        script_loc = '/home/danielc/bluse_telstate.py'
        # Timestamp for file name:
        script_time = datetime.utcnow()
        script_time = script_time.strftime("%Y%m%dT%H%M%S")
        output_file = '/home/danielc/solutions_{}.npz'.format(script_time)
        script_cmd = [script_env, 
                      script_loc,
                      '--telstate={}:{}'.format(endpoint_ip, endpoint_port),
                      '--output={}'.format(output_file)] 
        log.info('Running script: {}'.format(script_cmd))
        subprocess.Popen(script_cmd)

        # Save to Redis
        # File location:
        solutions_file_key = 'array_{}:kgball_file:{}'.format(subarray_number, script_time)
        redis_server.set(solutions_file_key, output_file)

        # K, G, B, all
        time.sleep(10) # Wait for script 
        solutions_output = np.load(output_file, allow_pickle=True)

        cal_keys = ['cal_K', 'cal_G', 'cal_B', 'cal_all']
        for i in range(len(cal_keys)):
            log.info(solutions_output['cal_K']) 

    # Fetch list of antennas associated with current subarray:
    ant_sensor = 'cbf_{}_receptors'.format(subarray_number)
    ant_details = io_loop.run_sync(lambda: fetch_sensor_pattern(ant_sensor, client, log))
    ant_list = []
    for sensor, details in ant_details.items():
        ant_list = details.value 
    if(len(ant_list) == 0):
        log.error('No antennas found in subarray {}. Note assuming CBF component is CBF and not CBF_dev'.format(subarray_number))
        sys.exit('Aborting')
    ant_list = ant_list.split(',') 

    # Build and retrieve specified sensor data from each antenna:
    all_ant_output = {}
    for ant in ant_list:
        sensor_pattern_i = sensor_pattern.format(subarray_number, ant)
        ant_i_sensor = io_loop.run_sync(lambda: fetch_sensor_pattern(sensor_pattern_i, client, log))
        for sensor, details in ant_i_sensor.items():
            sensor_vals = details.value
            sensor_vals = ast.literal_eval(sensor_vals)
            all_ant_output[ant] = {sensor:sensor_vals}
        log.info('Results for {} retrieved'.format(ant))
    log.info('Saving output...')
    with open('{}.pkl'.format(outfile), 'wb') as f:
        pickle.dump(all_ant_output, f)

if(__name__ == '__main__'):
    cli()
