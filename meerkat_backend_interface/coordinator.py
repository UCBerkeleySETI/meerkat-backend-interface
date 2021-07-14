import time
from optparse import OptionParser
import yaml
import json
import logging
import sys
import redis
import numpy as np
import string
from meerkat_backend_interface import redis_tools
from meerkat_backend_interface.logger import log, set_logger

# Redis channels to listen to
ALERTS_CHANNEL = redis_tools.REDIS_CHANNELS.alerts
SENSOR_CHANNEL = redis_tools.REDIS_CHANNELS.sensor_alerts
TRIGGER_CHANNEL = redis_tools.REDIS_CHANNELS.triggermode
# Type of stream
STREAM_TYPE = 'cbf.antenna_channelised_voltage'
# F-engine mode (so far 'wide' and 'narrow' are known to be available)
FENG_TYPE = 'wide.antenna-channelised-voltage'
# Hashpipe-Redis gateway domain
HPGDOMAIN   = 'bluse'
# Safety margin for setting index of first packet to record.
PKTIDX_MARGIN = 1024
# Slack channel to publish to:
SLACK_CHANNEL = 'meerkat-obs-log'
# Redis channel to send messages to the Slack proxy
PROXY_CHANNEL = 'slack-messages'

class Coordinator(object):
    """This class is used to coordinate receiving and recording F-engine data
       during commensal observations with MeerKAT. It communicates with the 
       processing nodes via the Hashpipe-Redis gateway [1]. 

       The coordinator automatically assigns computing resources to each 
       subarray, and responds to observation stages for each. As the required 
       metadata becomes available, it is published to the appropriate processing
       nodes. 
   
       [1] For further information on the Hashpipe-Redis gateway messages, please 
       see appendix B in https://arxiv.org/pdf/1906.07391.pdf
    """

    def __init__(self, redis_port, cfg_file, triggermode):
        """Initialise the coordinator.

           Args:
               redis_port (str): Redis port to listen on. 
               cfg_file (str): path to the .yml configuration file which
               (among other things) provides a list of available hosts. 
               triggermode (str): the desired trigger mode on startup is 
               set. Options include:
                   \'idle\': PKTSTART will not be sent.
                   \'auto\': PKTSTART will be sent each time a target is tracked.
                   \'armed\': PKTSTART will only be sent for the next target. 
                   Thereafter, the state will transition to idle.'
        """
        self.red = redis.StrictRedis(port=redis_port, decode_responses=True)
        self.cfg_file = cfg_file
        self.triggermode = triggermode 
        log = set_logger(log_level = logging.DEBUG)

    def start(self):
        """Start the coordinator as follows:

           - The list of available Hashpipe instances and the number of streams per 
             instance are retrieved from the main configuration .yml file. 

           - The number of available instances/hosts is read from the appropriate Redis key. 
           
           - Subscriptions are made to the three Redis channels.  
           
           - Incoming messages trigger the appropriate function for the stage of the 
             observation. The contents of the messages (if any) are sent to the 
             appropriate function. 
        """        
        # Configure coordinator
        try:
            self.hashpipe_instances, self.streams_per_instance = self.config(self.cfg_file)
            log.info('Configured from {}'.format(self.cfg_file))
        except:
            log.warning('Configuration not updated; old configuration might be present.')
        # Attempt to read list of available hosts. If key does not exist, recreate from 
        # config file
        free_hosts = self.red.lrange('coordinator:free_hosts', 0, 
                self.red.llen('coordinator:free_hosts'))
        if(len(free_hosts) == 0):
            redis_tools.write_list_redis(self.red, 'coordinator:free_hosts', self.hashpipe_instances)
            log.info('First configuration - no list of available hosts. Retrieving from config file.')
        # Subscribe to the required Redis channels.
        ps = self.red.pubsub(ignore_subscribe_messages=True)
        ps.subscribe(ALERTS_CHANNEL)
        ps.subscribe(SENSOR_CHANNEL)
        ps.subscribe(TRIGGER_CHANNEL)
        # Process incoming Redis messages:
        try:
            for msg in ps.listen():
                msg_type, description, value = self.parse_redis_msg(msg)
                # If trigger mode is changed on the fly:
                if((msg_type == 'coordinator') & (description == 'trigger_mode')):
                    self.triggermode = value
                    self.red.set('coordinator:trigger_mode', value)
                    log.info('Trigger mode set to \'{}\''.format(value))
                # If all the sensor values required on configure have been
                # successfully fetched by the katportalserver
                elif(msg_type == 'conf_complete'):
                    self.conf_complete(description)
                # If the current subarray is deconfigured, instruct processing nodes
                # to unsubscribe from their respective streams.
                # Only instruct processing nodes in the current subarray to unsubscribe.
                # Likewise, release hosts only for the current subarray. 
                elif(msg_type == 'deconfigure'):
                    self.deconfigure(description) 
                # Handle the full data-suspect bitmask, one bit per polarisation
                # per F-engine.
                elif(msg_type == 'data-suspect'):
                    self.data_suspect(description, value)
                # If the current subarray has transitioned to 'track' - that is, 
                # the antennas are on source and trackign successfully. 
                elif(msg_type == 'tracking'):
                    # Note that the description field is equivalent to product_id 
                    # here:
                    self.tracking_start(description)
                # If the current subarray transitions out of the tracking state:
                elif(msg_type == 'not-tracking'):
                    self.tracking_stop(description) 
                # If pointing updates are received during tracking
                elif('pos_request_base' in description):
                    self.pointing_update(msg_type, description, value)
        except KeyboardInterrupt:
            log.info("Stopping coordinator")
            sys.exit(0)
        except Exception as e:
            log.error(e)
            sys.exit(1)
    
    def conf_complete(self, description):
        """This function is run when a new subarray is configured and the 
           katportal_server has retrieved all the associated metadata required 
           for the processing nodes to ingest and record data from the F-engines. 

           The required metadata is published to the Hashpipe-Redis gateway in 
           the key-value pair format described in Appendix B of: 
           https://arxiv.org/pdf/1906.07391.pdf

           Notably, the DESTIP value is set for each processing node - the IP 
           address of the multicast group it is to join. 

           Args:
               
               description (str): the second field of the Redis message, which 
               in this case is the name of the current subarray. 
        """
        # This is the identifier for the subarray that has completed configuration.
        product_id = description
        tracking = 0 # Initialise tracking state to 0
        log.info('New subarray built: {}'.format(product_id))
        # Get IP address offset (if there is one) for ingesting only a specific
        # portion of the full band.
        offset = self.ip_offset(product_id)
        # Initialise trigger mode (idle, armed or auto)
        self.red.set('coordinator:trigger_mode:{}'.format(description), self.triggermode)
        log.info('Trigger mode for {} on startup: {}'.format(description, self.triggermode))
        # Generate list of stream IP addresses and publish appropriate messages to 
        # processing nodes:
        addr_list, port, n_addrs, n_red_chans = self.ip_addresses(product_id, offset)
        # Allocate hosts:
        free_hosts = self.red.lrange('coordinator:free_hosts', 0, 
                self.red.llen('coordinator:free_hosts'))
        # Allocate hosts for the current subarray:
        if(len(free_hosts) == 0):
            log.warning("No free resources, cannot process data from {}".format(product_id))
        else:
            allocated_hosts = free_hosts[0:n_red_chans]
            redis_tools.write_list_redis(self.red, 
                    'coordinator:allocated_hosts:{}'.format(product_id), allocated_hosts)
            # Remove allocated hosts from list of available hosts
            # NOTE: in future, append/pop with Redis commands instead of write_list_redis
            if(len(free_hosts) < n_red_chans):
                log.warning("Insufficient resources to process full band for {}".format(product_id))
                free_hosts = [] # Empty
            else:
                free_hosts = free_hosts[n_red_chans:]
            redis_tools.write_list_redis(self.red, 'coordinator:free_hosts', free_hosts)
            log.info('Allocated {} hosts to {}'.format(n_red_chans, product_id))
            # Build list of Hashpipe-Redis Gateway channels to publish to:
            chan_list = self.host_list(HPGDOMAIN, allocated_hosts)
            # Apply to processing nodes
            # NOTE: can we address multiple processing nodes more easily?
            for i in range(len(chan_list)):
                # Port (BINDPORT)
                self.pub_gateway_msg(self.red, chan_list[i], 'BINDPORT', port, log, True)        
                # Total number of streams (FENSTRM)
                self.pub_gateway_msg(self.red, chan_list[i], 'FENSTRM', n_addrs, log, True)
                # Sync time (UNIX, seconds)
                t_sync = self.sync_time(product_id)
                self.pub_gateway_msg(self.red, chan_list[i], 'SYNCTIME', t_sync, log, True)
                # Centre frequency (FECENTER)
                fecenter = self.centre_freq(product_id) 
                self.pub_gateway_msg(self.red, chan_list[i], 'FECENTER', fecenter, log, True)
                # Total number of frequency channels (FENCHAN)    
                n_freq_chans = self.red.get('{}:n_channels'.format(product_id))
                self.pub_gateway_msg(self.red, chan_list[i], 'FENCHAN', n_freq_chans, log, True)
                # Coarse channel bandwidth (from F engines)
                # Note: no sign information! 
                # (CHAN_BW)
                chan_bw = self.coarse_chan_bw(product_id, n_freq_chans)
                self.pub_gateway_msg(self.red, chan_list[i], 'CHAN_BW', chan_bw, log, True) 
                # Number of channels per substream (HNCHAN)
                hnchan = self.chan_per_substream(product_id)
                self.pub_gateway_msg(self.red, chan_list[i], 'HNCHAN', hnchan, log, True)
                # Number of spectra per heap (HNTIME)
                hntime = self.spectra_per_heap(product_id)
                self.pub_gateway_msg(self.red, chan_list[i], 'HNTIME', hntime, log, True)
                # Number of ADC samples per heap (HCLOCKS)
                adc_per_heap = self.samples_per_heap(product_id, hntime)
                self.pub_gateway_msg(self.red, chan_list[i], 'HCLOCKS', adc_per_heap, log, True)
                # Number of antennas (NANTS)
                n_ants = self.antennas(product_id)
                self.pub_gateway_msg(self.red, chan_list[i], 'NANTS', n_ants, log, True)
                # Set PKTSTART to 0 on configure
                self.pub_gateway_msg(self.red, chan_list[i], 'PKTSTART', 0, log, True)
                # Number of streams for instance i (NSTRM)
                n_streams_per_instance = int(addr_list[i][-1])+1
                self.pub_gateway_msg(self.red, chan_list[i], 'NSTRM', n_streams_per_instance, 
                    log, True)
                # Absolute starting channel for instance i (SCHAN)
                s_chan = offset*int(hnchan) + i*n_streams_per_instance*int(hnchan)
                self.pub_gateway_msg(self.red, chan_list[i], 'SCHAN', s_chan, log, True)
                # Destination IP addresses for instance i (DESTIP)
                self.pub_gateway_msg(self.red, chan_list[i], 'DESTIP', addr_list[i], log, True)

    def tracking_start(self, product_id):
        """When a subarray is on source and begins tracking, and the F-engine
           data is trustworthy, this function instructs the processing nodes
           to begin recording data.

           Data recording is initiated by issuing a PKTSTART value to the 
           processing nodes in question via the Hashpipe-Redis gateway [1].
          
           In addition, other appropriate metadata is published to the 
           processing nodes via the Hashpipe-Redis gateway. 

           Args:
               
               product_id (str): name of current subarray. 
           
           [1] https://arxiv.org/pdf/1906.07391.pdf
        """
        # Get list of allocated hosts for this subarray:
        array_key = 'coordinator:allocated_hosts:{}'.format(product_id)
        allocated_hosts = self.red.lrange(array_key, 0, 
                self.red.llen(array_key))
        # Build list of Hashpipe-Redis Gateway channels to publish to:
        chan_list = self.host_list(HPGDOMAIN, allocated_hosts)
        # Send messages to these specific hosts:
        datadir = self.datadir(product_id, allocated_hosts)
        for i in range(len(chan_list)):
            # Publish DATADIR to gateway
            self.pub_gateway_msg(self.red, chan_list[i], 'DATADIR', datadir, 
                log, False)
            # Target information:
            target_str, ra_str, dec_str = self.target(product_id)
            # SRC_NAME:
            self.pub_gateway_msg(self.red, chan_list[i], 'SRC_NAME', target_str, 
                log, False)
            # RA_STR and DEC_STR 
            self.pub_gateway_msg(self.red, chan_list[i], 'RA_STR', ra_str, 
                log, False)
            self.pub_gateway_msg(self.red, chan_list[i], 'DEC_STR', dec_str, 
                log, False)
        # Set PKTSTART separately after all the above messages have 
        # all been delivered:
        pkt_idx_start = self.get_start_idx(allocated_hosts, PKTIDX_MARGIN, log)
        for i in range(len(chan_list)):
            self.pub_gateway_msg(self.red, chan_list[i], 'PKTSTART', 
                pkt_idx_start, log, False)
        # Alert via slack:
        slack_message = "{}::meerkat:: New recording started for {}!".format(SLACK_CHANNEL, product_id)
        self.red.publish(PROXY_CHANNEL, slack_message)
        # If armed, reset triggermode to idle after triggering 
        # once.
        # NOTE: need to fix triggermode retrieval? Perhaps done?
        triggermode = self.red.get('coordinator:trigger_mode:{}'.format(product_id))
        if(triggermode == 'armed'):
            self.red.set('coordinator:trigger_mode:{}'.format(product_id), 'idle')
            log.info('Triggermode set to \'idle\' from \'armed\' from {}'.format(product_id))
        elif('nshot' in triggermode):
            nshot = triggermode.split(':')
            n = int(nshot[1]) - 1
            triggermode = '{}:{}'.format(nshot[0], n)
            # If nshot mode, decrement nshot by one and write to Redis. 
            self.red.set('coordinator:trigger_mode:{}'.format(product_id), triggermode)
            log.info('Triggermode: n shots remaining: {}'.format(n))
            if(n <= 0):
                # Set triggermode to idle. 
                triggermode = 'idle'
                self.red.set('coordinator:trigger_mode:{}'.format(product_id), 'idle')
                log.info('Triggermode set to \'idle\' from \'nshot\'')
        # Set subarray state to 'tracking'
        self.red.set('coordinator:tracking:{}'.format(product_id), '1')

    def tracking_stop(self, product_id):
        """If the subarray stops tracking a source (more specifically, if the incoming 
           data is no longer to be trusted or used), the following actions are taken:

           - DWELL is set to 0
           - PKTSTART is set to 0
           - DWELL is reset to its original value. 

           This ensures that the processing nodes stop recording (if DWELL has not yet
           already been reached). 

           Args:
               product_id (str): the name of the current subarray.
        """
        tracking_state = self.red.get('coordinator:tracking:{}'.format(product_id))
        # If tracking state transitions from 'track' to any of the other states, 
        # follow the procedure below. Otherwise, do nothing.  
        if(tracking_state == '1'):
            # Get list of allocated hosts for this subarray:
            array_key = 'coordinator:allocated_hosts:{}'.format(product_id)
            allocated_hosts = self.red.lrange(array_key, 0, 
                self.red.llen(array_key))
            # Build list of Hashpipe-Redis Gateway channels to publish to:
            chan_list = self.host_list(HPGDOMAIN, allocated_hosts)
            # Send messages to these specific hosts:
            for i in range(len(chan_list)):
                # For the moment during testing, get dwell time from each
                # of the hosts. Then set to zero and then back to to the
                # original dwell time.
                host_key = '{}://{}/status'.format(HPGDOMAIN, allocated_hosts[i])
                dwell_time = self.get_dwell_time(host_key)
                self.pub_gateway_msg(self.red, chan_list[i], 'DWELL', '0', log, False)
                self.pub_gateway_msg(self.red, chan_list[i], 'PKTSTART', '0', log, False)
                time.sleep(0.1) # Wait for processing node. NOTE: Is this long enough?
                self.pub_gateway_msg(self.red, chan_list[i], 'DWELL', dwell_time, log, False)
            # Reset tracking state to '0'
            self.red.set('coordinator:tracking:{}'.format(product_id), '0')

    def deconfigure(self, description):
        """If the current subarray is deconfigured, the following steps are taken:
           
           - For the hosts associated with the current subarray, DESTIP is set to 
             0.0.0.0 - this ensures that they unsubscribe from the multicast group
             and stop receiving raw voltage data from the F-engines. 

           - These hosts are then released back into the pool of available hosts 
             for allocation to other subarrays. 

           Args:
              description (str): the name of the current subarray. 
        """
        # Fetch hosts allocated to this subarray:
        # Note description equivalent to product_id here
        array_key = 'coordinator:allocated_hosts:{}'.format(description)
        allocated_hosts = self.red.lrange(array_key, 0, 
                self.red.llen(array_key))
        # Build list of Hashpipe-Redis Gateway channels to publish to:
        chan_list = self.host_list(HPGDOMAIN, allocated_hosts)
        # Send deconfigure message to these specific hosts:
        for i in range(len(chan_list)):
            self.pub_gateway_msg(self.red, chan_list[i], 'DESTIP', '0.0.0.0', log, False)
        log.info('Subarray {} deconfigured'.format(description))
        # Release hosts:
        # NOTE: in future, get rid of write_list_redis function and append or pop. 
        # This will simplify this step. 
        # Get list of currently available hosts:
        free_hosts = self.red.lrange('coordinator:free_hosts', 0, 
                self.red.llen('coordinator:free_hosts'))
        # Append released hosts and write 
        free_hosts = free_hosts + allocated_hosts
        redis_tools.write_list_redis(self.red, 'coordinator:free_hosts', free_hosts)
        # Remove resources from current subarray 
        self.red.delete('coordinator:allocated_hosts:{}'.format(description))
        log.info("Released {} hosts; {} hosts available".format(len(allocated_hosts), 
                len(free_hosts)))
                    
    def data_suspect(self, description, value): 
        """Parse and publish data-suspect mask to the appropriate 
        processing nodes.

        The data-suspect mask provides a global indication of whether or not
        the data from each polarisation from each F-engine can be trusted. A 
        number of parameters are included in this determination (including 
        whether or not a source is being tracked). Please see MeerKAT CAM 
        documentation for further information. 

        Args:
            description (str): the name of the current subarray. 
            value (str): the data-suspect bitmask. 
        """
        bitmask = '#{:x}'.format(int(value, 2))
        # Fetch hosts allocated to this subarray:
        # Note description equivalent to product_id here
        array_key = 'coordinator:allocated_hosts:{}'.format(description)
        allocated_hosts = self.red.lrange(array_key, 0, self.red.llen(array_key))
        chan_list = self.host_list(HPGDOMAIN, allocated_hosts)
        for i in range(len(chan_list)):
            # NOTE: Question: do we want to publish the entire bitmask to each 
            # processing node?
            self.pub_gateway_msg(self.red, chan_list[i], 'FESTATUS', bitmask, log, False)

    def pointing_update(self, msg_type, description, value):
        """Update pointing information during an observation, and publish 
        results into the Hashpipe-Redis gateway status buffer for the specific
        set of processing nodes allocated to the current subarray. These values 
        include RA, Dec, Az and El and are updated continuously as they change.

        Args:
           msg_type (str): currently indicates the name of the current subarray. 
           (to change in future for consistency). 
           description (str): type of pointing information to update. 
           value (str): the value of the pointing information to update. 
        """
        # NOTE: here, msg_type represents product_id. Need to fix this inconsistent
        # naming convention. 
        # Fetch hosts allocated to this subarray:
        array_key = 'coordinator:allocated_hosts:{}'.format(msg_type)
        allocated_hosts = self.red.lrange(array_key, 0, 
                self.red.llen(array_key))
        # Build list of Hashpipe-Redis Gateway channels to publish to:
        chan_list = self.host_list(HPGDOMAIN, allocated_hosts)
        # Send deconfigure message to these specific hosts:
        # RA and Dec (in degrees)
        if('dec' in description):
            for i in range(len(chan_list)):
                self.pub_gateway_msg(self.red, chan_list[i], 'DEC', value, log, False)
        elif('ra' in description):
            for i in range(len(chan_list)):
                # pos_request_base_ra value is given in hrs (single float
                # value)
                ra_deg = float(value)*15.0 # Convert to degrees
                self.pub_gateway_msg(self.red, chan_list[i], 'RA', ra_deg, log, False)
        # Azimuth and elevation (in degrees):
        elif('azim' in description):
            for i in range(len(chan_list)):
                self.pub_gateway_msg(self.red, chan_list[i], 'AZ', value, log, False)
        elif('elev' in description):
            for i in range(len(chan_list)):
                self.pub_gateway_msg(self.red, chan_list[i], 'EL', value, log, False)

    def get_dwell_time(self, host_key):
        """Get the current dwell time from the status buffer
        stored in Redis for a particular host. 

        Args:
            host_key (str): Key for Redis hash of status buffer
            for a particular host.
        
        Returns:
            dwell_time (int): Dwell time (recording length) in
            seconds.
        """
        dwell_time = 0
        host_status = self.red.hgetall(host_key)
        if(len(host_status) > 0):
            if('DWELL' in host_status):
                dwell_time = host_status['DWELL']
            else:
                log.warning('DWELL is missing for {}'.format(host_key))
        else:
            log.warning('Cannot acquire {}'.format(host_key))
        return dwell_time

    def get_pkt_idx(self, host_key):
        """Get PKTIDX for a host (if active).
        
        Args:
            red_server: Redis server.
            host_key (str): Key for Redis hash of status buffer for a 
            particular active host.
    
        Returns:
            pkt_idx (str): Current packet index (PKTIDX) for a particular 
            active host. Returns None if host is not active.
        """
        pkt_idx = None
        host_status = self.red.hgetall(host_key)
        if(len(host_status) > 0):
            if('NETSTAT' in host_status):
                if(host_status['NETSTAT'] != 'idle'):
                    if('PKTIDX' in host_status):
                        pkt_idx = host_status['PKTIDX']
                    else:
                        log.warning('PKTIDX is missing for {}'.format(host_key))
                else:
                    log.warning('NETSTAT is missing for {}'.format(host_key))
        else:
            log.warning('Cannot acquire {}'.format(host_key))
        return pkt_idx

    def get_start_idx(self, host_list, idx_margin, log):
        """Calculate the packet index at which recording should begin
        (synchronously) for all processing nodes.
    
            Args:
                red_server: Redis server.
                host_list (List): List of host/processing node names (incuding
                instance number).
                idx_margin (int): The safety margin (number of extra packets
                before beginning to record) to ensure a synchronous start across
                processing nodes.
                log: Logger.
            
            Returns:
                start_pkt (int): The packet index at which to begin recording
                data.
        """
        pkt_idxs = []
        for host in host_list:
            host_key = '{}://{}/status'.format(HPGDOMAIN, host)
            pkt_idx = self.get_pkt_idx(host_key)
            if(pkt_idx is not None):
                pkt_idxs = pkt_idxs + [pkt_idx]
        if(len(pkt_idxs) > 0):
            start_pkt = self.select_pkt_start(pkt_idxs, log, idx_margin)
            return start_pkt
        else:
            log.warning('No active processing nodes. Cannot set PKTIDX')
            return None

    def select_pkt_start(self, pkt_idxs, log, idx_margin):
        """Calculates the index of the first packet from which to record
        for each processing node.
        Employs rudimentary statistics on packet indices to determine
        a sensible starting packet for all processing nodes.

        Args:
            pkt_idxs (list): List of the packet indices from each active host.
            log: Logger.
            idx_margin (int): The safety margin (number of extra packets
            before beginning to record) to ensure a synchronous start across
            processing nodes.
        
        Returns:
            start_pkt (int): The packet index at which to begin recording
            data.
        """
        pkt_idxs = np.asarray(pkt_idxs, dtype = np.int64)
        median = np.median(pkt_idxs)
        margin_diff = np.abs(pkt_idxs - median)
        # Using idx_margin as safety margin
        margin = idx_margin
        outliers = np.where(margin_diff > margin)[0]
        n_outliers = len(outliers)
        if(n_outliers > 0):
            log.warning('{} PKTIDX value(s) exceed margin.'.format(n_outliers))
        if(n_outliers > len(pkt_idxs)/2):
            log.warning('Large PKTIDX spread. Check PKTSTART value.')
        # Find largest value less than margin
        margin_diff[outliers] = 0
        max_idx = np.argmax(margin_diff)
        start_pkt = pkt_idxs[max_idx] + idx_margin
        return start_pkt

    def host_list(self, hpgdomain, hosts):
        """Build a list of Hashpipe-Redis Gateway channels from a list 
           of host names.

           Args:
              hpgdomain (str): Hashpipe-Redis Gateway domain (e.g. 'bluse'). 
              hosts (list): list of hosts.

           Returns:
              channel_list (list): list of Hashpipe-Redis Gateway channels.
        """
        channel_list = [hpgdomain + '://' + host + '/set' for host in hosts]
        return channel_list

    def target(self, product_id):
        """Get target name and coordinates.

           Args:
              product_id (str): the name of the current subarray.

           Returns:
              target_str (str): target string including name/description. 
              ra_str (str): RA of current pointing in sexagesimal form.
              dec_str (str): Dec of current pointing in sexagesimal form. 
        """
        ant_key = '{}:antennas'.format(product_id)
        ant_list = self.red.lrange(ant_key, 0, self.red.llen(ant_key))
        target_key = "{}:{}_target".format(product_id, ant_list[0])
        target_str = self.get_target(product_id, target_key, 5, 15)
        target_str, ra_str, dec_str = self.target_name(target_str, 16, delimiter = "|")
        return target_str, ra_str, dec_str

    def target_name(self, target_string, length, delimiter = "|"):
        """Limit target description length and replace punctuation with dashes for
        compatibility with filterbank/raw file header requirements. All contents 
        up to the stop character are kept.

        Args:
            target_string (str): Target description string from CBF. A typical 
            example:
            "J0918-1205 | Hyd A | Hydra A | 3C 218 | PKS 0915-11, radec, 
            9:18:05.28, -12:05:48.9"
            length (int): Maximum length for target description.
            delimiter (str): Character at which to split the target string. 
        
        Returns:
            target: Formatted target description suitable for 
            filterbank/raw headers.
            ra_str: RA_STR as accessed from target string.
            dec_str: DEC_STR as accessed from target string.
        """ 
        # Assuming target name or description will always come first
        # Remove any outer single quotes for compatibility:
        target = target_string.strip('\'')
        if('radec target' in target):
            target = target.split('radec target,') # Split at 'radec target'
        else:
            target = target.split('radec,') # Split at 'radec tar'
        # Target:
        if(target[0].strip() == ''): # if no target field
            # strip twice for python compatibility
            target_name = 'NOT_PROVIDED'
        else:
            target_name = target[0].split(delimiter)[0] # Split at specified delimiter
            target_name = target_name.strip() # Remove leading and trailing whitespace
            target_name = target_name.strip(",") # Remove trailing comma
            # Punctuation below taken from string.punctuation()
            # Note that + and - have been removed (relevant to coordinate names)
            punctuation = "!\"#$%&\'()*,./:;<=>?@[\\]^_`{|}~" 
            # Replace all punctuation with underscores
            table = str.maketrans(punctuation, '_'*30)
            target_name = target_name.translate(table)
            # Limit target string to max allowable in headers (68 chars)
            target_name = target_name[0:length]
        # RA_STR and DEC_STR
        radec = target[1].split(',')
        ra_str = radec[0].strip()
        dec_str = radec[1].strip()
        return(target_name, ra_str, dec_str)

    def get_target(self, product_id, target_key, retries, retry_duration):
        """Try to fetch the most recent target name by comparing its timestamp
           to that of the most recent capture-start message.

           Args:
              product_id (str): name of the current subarray. 
              target_key (str): Redis key for the current target. 
              retries (int): number of times to attempt fetching the new 
              target name. 
              retry_duration (float): time (s) to wait between retries. 
           
           Returns:
               target (str): current target name - defaults to 'UNKNOWN' 
               if no new target name is available. 
        """
        for i in range(retries):
            last_target = float(self.red.get("{}:last-target".format(product_id)))
            last_start = float(self.red.get("{}:last-capture-start".format(product_id)))
            if((last_target - last_start) < 0): # Check if new target available
                log.warning("No new target name, retrying.")
                time.sleep(retry_duration)
                continue
            else:
                break
        if(i == (retries - 1)):
            log.error("No new target name after {} retries; defaulting to UNKNOWN".format(retries))
            target = 'UNKNOWN'
        else:
            target = self.red.get(target_key)
        return target 

    def config(self, cfg_file):
        """Configure the coordinator according to .yml config file.

        Args:
            cfg_file (str): File path for config file (.yml).
        
        Returns:
            List of instances and the number of streams to be processed per 
            instance.
        """
        try:
            with open(cfg_file, 'r') as f:
                try:
                    cfg = yaml.safe_load(f)
                    return(cfg['hashpipe_instances'], 
                        cfg['streams_per_instance'][0])
                except yaml.YAMLError as E:
                    log.error(E)
        except IOError:
            log.error('Config file not found')

    def pub_gateway_msg(self, red_server, chan_name, msg_name, msg_val, logger, write):
        """Format and publish a hashpipe-Redis gateway message. Save messages
        in a Redis hash for later use by reconfig tool. 
        
        Args:
            red_server: Redis server.
            chan_name (str): Name of channel to be published to. 
            msg_name (str): Name of key in status buffer.
            msg_val (str): Value associated with key.
            logger: Logger. 
            write (bool): If true, also write message to Redis database.
        """
        msg = '{}={}'.format(msg_name, msg_val)
        red_server.publish(chan_name, msg)
        # save hash of most recent messages
        if(write):
            red_server.hset(chan_name, msg_name, msg_val)
            logger.info('Wrote {} for channel {} to Redis'.format(msg, chan_name))
        logger.info('Published {} to channel {}'.format(msg, chan_name))

    def parse_redis_msg(self, message):
        """Process incoming Redis messages from the various pub/sub channels. 
           Messages are formatted as follows:
           
               <message_type>:<description>:<value>
           
           OR (if there is no associated value): 

               <message_type>:<description>

           If the message does not appear to fit the format, returns an 
           empty string.

           Args:
              message (str): the incoming Redis message. 

           Returns:
              msg_type (str): type of incoming message (eg 'deconfigure')
              description (str): description of incoming message (eg
              'pos_request_base_ra')
              value (str): value associated with incoming message (eg 
              '14:24:32.24'
        """
        msg_type = ''
        description = ''
        value = ''
        msg_parts = message['data'].split(':', 2)
        if len(msg_parts) < 2:
            log.info("Not processing this message: {}".format(message))
        else:
            msg_type = msg_parts[0]
            description = msg_parts[1] 
        if(len(msg_parts) > 2):
            value = msg_parts[2]
        return msg_type, description, value        

    def cbf_sensor_name(self, product_id, sensor):
        """Builds the full name of a CBF sensor according to the 
        CAM convention.  
  
        Args:
            product_id (str): Name of the current active subarray.
            sensor (str): Short sensor name (from the .yml config file).
 
        Returns:
            cbf_sensor (str): Full cbf sensor name for querying via KATPortal.
        """
        cbf_name = self.red.get('{}:cbf_name'.format(product_id))
        cbf_prefix = self.red.get('{}:cbf_prefix'.format(product_id))
        cbf_sensor_prefix = '{}:{}_{}_'.format(product_id, cbf_name, cbf_prefix)
        cbf_sensor = cbf_sensor_prefix + sensor
        return cbf_sensor

    def stream_sensor_name(self, product_id, sensor):
        """Builds the full name of a stream sensor according to the 
        CAM convention.  
        
        Args:
            product_id (str): Name of the current active subarray.
            sensor (str): Short sensor name (from the .yml config file).

        Returns:
            stream_sensor (str): Full stream sensor name for querying 
            via KATPortal.
        """
        s_num = product_id[-1] # subarray number
        cbf_prefix = self.red.get('{}:cbf_prefix'.format(product_id))
        stream_sensor = '{}:subarray_{}_streams_{}_{}'.format(product_id, 
            s_num, cbf_prefix, sensor)
        return stream_sensor

    def datadir(self, product_id, host_list):
        """Determine DATADIR according to the current schedule block ID. This 
           entails retrieving the list of schedule blocks, extracting the ID of 
           the current one and formatting it as a file path.
        
           Schedule block IDs follow the format: YYYYMMDD-XXXX where XXXX is the 
           schedule block number (in order of assignment). To create the correct
           DATADIR, we format it (per schedule block) as follows: 
           DATADIR=YYYYMMDD/XXXX
           If the schedule block ID is not known, we set it to:
           DATADIR=Unknown_SB

           Args:
              product_id (str): the name of the current subarray

           Returns:
              datadir (str): the name of the directory in which to record
              incoming data. 
        """
        current_sb_id = 'Unknown_SB' # Default
        # Attempt to fetch the current upper directory for the data:
        upper_dir = self.get_datadir_root(host_list)
        # Attempt to fetch current SB 
        try: 
            sb_key = '{}:sched_observation_schedule_1'.format(product_id)
            sb_ids = self.red.get(sb_key)
            # First ID in list is the current SB (as per CAM GUI)
            current_sb_id = sb_ids.split(',')[0] 
            # Format for file path
            current_sb_id = current_sb_id.replace('-', '/')
        except:
            log.error("Schedule block IDs not available")
            log.warning("Setting DATADIR='/{}/Unknown_SB".format(upper_dir))
        datadir = '/{}/{}'.format(upper_dir, current_sb_id)
        return datadir

    def get_datadir_root(self, host_list):
        """Get the upper directory for DATADIR from the status buffers if 
           available.
        """
        host_key = '{}://{}/status'.format(HPGDOMAIN, host_list[0])
        host_status = self.red.hgetall(host_key)
        upper_dir = 'buf0' # default to NVMe modules
        if(len(host_status) > 0):
            if('DATADIR' in host_status):
                if(len(host_status['DATADIR']) > 0):
                    upper_dir = host_status['DATADIR']
                else:
                    log.warning('No preset DATADIR for {}, defaulting to /buf0/'.format(host_key))
            else:
                log.warning('DATADIR not available for {}, defaulting to /buf0/'.format(host_key))
        else:
            log.warning('Cannot acquire {}, defaulting to /buf0/'.format(host_key))
        return upper_dir

    def antennas(self, product_id):
        """Number of antennas, NANTS.

           Args:
              product_id (str): the name of the current subarray.

           Returns:
              n_ants (int): the number of antennas in the current subarray. 
        """
        ant_key = '{}:antennas'.format(product_id)
        n_ants = len(self.red.lrange(ant_key, 0, self.red.llen(ant_key)))
        return n_ants
        
    def chan_per_substream(self, product_id):
        """Number of channels per substream - equivalent to HNCHAN.

           Args:
              product_id (str): the name of the current subarray.

           Returns:
              n_chans_per_substream (str): the number of channels per substream. 
        """
        sensor_key = self.cbf_sensor_name(product_id, 
                'antenna_channelised_voltage_n_chans_per_substream')   
        n_chans_per_substream = self.red.get(sensor_key)
        return n_chans_per_substream

    def spectra_per_heap(self, product_id):
        """Number of spectra per heap (HNTIME). Please see [1] for further 
           information on the SPEAD protocol.

           [1] Manley, J., Welz, M., Parsons, A., Ratcliffe, S., & 
           Van Rooyen, R. (2010). SPEAD: streaming protocol for exchanging 
           astronomical data. SKA document.

           Args:
              product_id (str): the name of the current subarray.

           Returns:
              spectra_per_heap (str): the number of spectra per heap.
        """
        sensor_key = self.cbf_sensor_name(product_id,  
            'tied_array_channelised_voltage_0x_spectra_per_heap')   
        spectra_per_heap = self.red.get(sensor_key)
        return spectra_per_heap

    def coarse_chan_bw(self, product_id, n_freq_chans):
        """Coarse channel bandwidth (from F engines).
           NOTE: no sign information! Equivalent to CHAN_BW.
           
           Args:
              product_id (str): the name of the current subarray.
              n_freq_chans (str): the number of coarse channels

           Returns:
              coarse_chan_bw (float): coarse channel bandwidth. 
        """
        sensor_key = self.cbf_sensor_name(product_id, 
            'adc_sample_rate')
        adc_sample_rate = self.red.get(sensor_key)
        coarse_chan_bw = float(adc_sample_rate)/2.0/int(n_freq_chans)/1e6
        coarse_chan_bw = '{0:.17g}'.format(coarse_chan_bw)
        return coarse_chan_bw

    def centre_freq(self, product_id):
        """Centre frequency (FECENTER).
           
           Args:
              product_id (str): the name of the current subarray.

           Returns:
              centre_freq (float): centre frequency of the current subarray.
        """
        sensor_key = self.stream_sensor_name(product_id,
            'antenna_channelised_voltage_centre_frequency')
        centre_freq = self.red.get(sensor_key)
        centre_freq = float(centre_freq)/1e6
        centre_freq = '{0:.17g}'.format(centre_freq)
        return centre_freq

    def sync_time(self, product_id):
        """Sync time (UNIX, seconds)
        """
        sensor_key = self.cbf_sensor_name(product_id, 'sync_time')   
        sync_time = int(float(self.red.get(sensor_key))) # Is there a cleaner way?
        return sync_time

    def samples_per_heap(self, product_id, spectra_per_heap):
        """Equivalent to HCLOCKS.
        """
        sensor_key = self.cbf_sensor_name(product_id,
            'antenna_channelised_voltage_n_samples_between_spectra')
        adc_per_spectra = self.red.get(sensor_key)
        adc_per_heap = int(adc_per_spectra)*int(spectra_per_heap)
        return adc_per_heap

    def ip_offset(self, product_id):
        """Get IP offset (for ingesting fractions of the band)
        """
        try:
            offset = int(self.red.get('{}:ip_offset'.format(product_id)))
            if(offset > 0):
                log.info('Stream IP offset applied: {}'.format(offset))
        except:
            log.info("No stream IP offset; defaulting to 0")
            offset = 0
        return offset

    def ip_addresses(self, product_id, offset):
        """Acquire and apportion multicast IP groups.
        """
        all_streams = json.loads(self.json_str_formatter(self.red.get(
            "{}:streams".format(product_id))))
        streams = all_streams[STREAM_TYPE]
        stream_addresses = streams[FENG_TYPE]
        addr_list, port, n_addrs = self.read_spead_addresses(stream_addresses, 
            len(self.hashpipe_instances), 
            self.streams_per_instance, offset)
        n_red_chans = len(addr_list)
        return addr_list, port, n_addrs, n_red_chans

    def json_str_formatter(self, str_dict):
        """Formatting for json.loads
        
        Args:
            str_dict (str): str containing dict of spead streams (received 
            on ?configure).
        
        Returns:
            str_dict (str): str containing dict of spead streams, formatted 
            for use with json.loads
        """
        # Is there a better way of doing this?
        str_dict = str_dict.replace('\'', '"')  # Swap quote types for json format
        str_dict = str_dict.replace('u', '')  # Remove unicode 'u'
        return str_dict

    def read_spead_addresses(self, spead_addrs, n_groups, streams_per_instance, offset):
        """Parses spead addresses given in the format: spead://<ip>+<count>:<port>
        Assumes this format.
        
        Args:
            spead_addrs (str): string containing spead IP addresses in the format 
            above.
            n_groups (int): number of stream addresses to be sent to each 
            processing instance.
            offset (int): number of streams to skip before apportioning IPs.
        
        Returns:
            addr_list (list): list of spead stream IP address groups.
            port (int): port number.
        """
        addrs = spead_addrs.split('/')[-1]
        addrs, port = addrs.split(':')
        try:
            addr0, n_addrs = addrs.split('+')
            n_addrs = int(n_addrs) + 1
            addr_list = self.create_addr_list_filled(addr0, n_groups, n_addrs, 
                streams_per_instance, offset)
        except ValueError:
            addr_list = [addrs + '+0']
            n_addrs = 1
        return addr_list, port, n_addrs

    def create_addr_list_filled(self, addr0, n_groups, n_addrs, streams_per_instance, offset):
        """Creates list of IP multicast subscription address groups.
        Fills the list for each available processing instance 
        sequentially untill all streams have been assigned.
        
        Args:
            addr0 (str): IP address of the first stream.
            n_groups (int): number of available processing instances.
            n_addrs (int): total number of streams to process.
            streams_per_instance (int): number of streams to be processed 
            by each instance.
            offset (int): number of streams to skip before apportioning
            IPs.

        Returns:
            addr_list (list): list of IP address groups for subscription.
        """
        prefix, suffix0 = addr0.rsplit('.', 1)
        suffix0 = int(suffix0) + offset
        n_addrs = n_addrs - offset
        addr_list = []
        if(n_addrs > streams_per_instance*n_groups):
            log.warning('Too many streams: {} will not be processed.'.format(
                n_addrs - streams_per_instance*n_groups))
            for i in range(0, n_groups):
                addr_list.append(prefix + '.{}+{}'.format(
                    suffix0, streams_per_instance - 1))
                suffix0 = suffix0 + streams_per_instance
        else:
            n_instances_req = int(np.ceil(n_addrs/float(streams_per_instance)))
            for i in range(1, n_instances_req):
                addr_list.append(prefix + '.{}+{}'.format(suffix0, 
                    streams_per_instance - 1))
                suffix0 = suffix0 + streams_per_instance
            addr_list.append(prefix + '.{}+{}'.format(suffix0, 
                n_addrs - 1 - i*streams_per_instance))
        return addr_list
