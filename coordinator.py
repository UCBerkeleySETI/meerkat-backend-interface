#!/usr/bin/env python

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

def get_pkt_idx(red_server, host_key):
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
    host_status = red_server.hgetall(host_key)
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

def get_dwell_time(red_server, host_key):
    """Get the current dwell time from the status buffer
    stored in Redis for a particular host. 

    Args:
        red_server: Redis server. 
        host_key (str): Key for Redis hash of status buffer
        for a particular host.

    Returns:
        dwell_time (int): Dwell time (recording length) in
        seconds.
    """
    dwell_time = 0
    host_status = red_server.hgetall(host_key)
    if(len(host_status) > 0):
        if('DWELL' in host_status):
            dwell_time = host_status['DWELL']
        else:
            log.warning('DWELL is missing for {}'.format(host_key))
    else:
        log.warning('Cannot acquire {}'.format(host_key))
    return dwell_time

def select_pkt_start(pkt_idxs, log, idx_margin):
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

def get_start_idx(red_server, host_list, idx_margin, log):
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
        pkt_idx = get_pkt_idx(red_server, host_key)
        if(pkt_idx is not None):
            pkt_idxs = pkt_idxs + [pkt_idx]
    if(len(pkt_idxs) > 0):
        start_pkt = select_pkt_start(pkt_idxs, log, idx_margin)
        return start_pkt
    else:
        log.warning('No active processing nodes. Cannot set PKTIDX')   
        return None

def json_str_formatter(str_dict):
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

def create_addr_list_filled(addr0, n_groups, n_addrs, streams_per_instance, offset):
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

def create_addr_list_distributed(addr0, n_groups, n_addrs):
    """Creates list of IP multicast subscription address groups.
    Attempts to divide the number of streams equally by the number
    of available processing instances.     

    Args:
        addr0 (str): first IP address in the list.
        n_groups (int): number of available processing instances.
        n_per_group (int): number of SPEAD stream addresses per instance.

    Returns:
        addr_list (list): list of IP address groups for subscription.
    """
    prefix, suffix0 = addr0.rsplit('.', 1)
    addr_list = []
    extra_addrs = n_addrs%n_groups
    n_per_group = np.ones(n_groups, dtype=int)*(n_addrs/n_groups)
    n_per_group[:extra_addrs] += 1
    for i in range(0, min(n_addrs, n_groups)):
        addr_list.append(prefix + '.{}'.format(int(suffix0)) + '+' 
            + str(n_per_group[i]-1))
        suffix0 = int(suffix0) + n_per_group[i]
    return addr_list

def read_spead_addresses(spead_addrs, n_groups, streams_per_instance, offset):
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
        addr_list = create_addr_list_filled(addr0, n_groups, n_addrs, 
            streams_per_instance, offset)
    except ValueError:
        addr_list = [addrs + '+0']
        n_addrs = 1
    return addr_list, port, n_addrs

def cli():
    """Command line interface. 
    """
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-p', '--port', dest='port', type=int,
                      help='Redis port to connect to', default=6379)
    parser.add_option('-c', '--config', dest='cfg_file', type=str,
                      help='Config filename (yaml)', default = 'config.yml')
    parser.add_option('-t', '--triggermode', dest='triggermode', type=str,
                      help="""Trigger mode: 
                                  \'idle\': PKTSTART will not be sent.
                                  \'auto\': PKTSTART will be sent each 
                                      time a target is tracked.
                                  \'armed\': PKTSTART will only be sent for 
                                      the next target. Thereafter, the state 
                                      will transition to idle.'
                           """,
                      default = 'auto')
    (opts, args) = parser.parse_args()
    # if not opts.port:
    #     print "MissingArgument: Port number"
    #     sys.exit(-1)
    main(port=opts.port, cfg_file=opts.cfg_file, triggermode=opts.triggermode)

def configure(cfg_file):
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

def pub_gateway_msg(red_server, chan_name, msg_name, msg_val, logger, write):
    """Format and publish a hashpipe-Redis gateway message. Save messages
    in a Redis hash for later use by reconfig tool. 

    Args:
        red_server: Redis server.
        chan_name (str): Name of channel to be published to. 
        msg_name (str): Name of key in status buffer.
        msg_val (str): Value associated with key.
        logger: Logger. 
        write (bool): If true, also write message to Redis database.

    Returns:
        None
    """
    msg = '{}={}'.format(msg_name, msg_val)
    red_server.publish(chan_name, msg)
    # save hash of most recent messages
    if(write):
        red_server.hset(chan_name, msg_name, msg_val)
        logger.info('Wrote {} for channel {} to Redis'.format(msg, chan_name))
    logger.info('Published {} to channel {}'.format(msg, chan_name))

def cbf_sensor_name(product_id, redis_server, sensor):
    """Builds the full name of a CBF sensor according to the 
    CAM convention.  

    Args:
        product_id (str): Name of the current active subarray.
        redis_server: Redis server.
        sensor (str): Short sensor name (from the .yml config file).

    Returns:
        cbf_sensor (str): Full cbf sensor name for querying via KATPortal.
    """
    cbf_name = redis_server.get('{}:cbf_name'.format(product_id))
    cbf_prefix = redis_server.get('{}:cbf_prefix'.format(product_id))
    cbf_sensor_prefix = '{}:{}_{}_'.format(product_id, cbf_name, cbf_prefix)
    cbf_sensor = cbf_sensor_prefix + sensor
    return cbf_sensor

def stream_sensor_name(product_id, redis_server, sensor):
    """Builds the full name of a stream sensor according to the 
    CAM convention.  

    Args:
        product_id (str): Name of the current active subarray.
        redis_server: Redis server.
        sensor (str): Short sensor name (from the .yml config file).

    Returns:
        stream_sensor (str): Full stream sensor name for querying 
        via KATPortal.
    """
    s_num = product_id[-1] # subarray number
    cbf_prefix = redis_server.get('{}:cbf_prefix'.format(product_id))
    stream_sensor = '{}:subarray_{}_streams_{}_{}'.format(product_id, 
        s_num, cbf_prefix, sensor)
    return stream_sensor

def target_name(target_string, length, delimiter = "|"):
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
        target = target.split('radec target,') # Split at 'radec tar'
    else:
        target = target.split('radec,') # Split at 'radec tar'
    # Target:
    if(target[0].strip() == ''): # if no target field
        # strip twice for python compatibility
        target_name = 'NOT_PROVIDED'
    else:
        target_name = target[0].split(delimiter)[0] # Split at specified delimiter
        target_name = target_name.strip() # Remove leading and trailing whitespace
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

def get_target(product_id, target_key, retries, retry_duration, redis_server, log):
    """
    Try to fetch the most recent target name by comparing its timestamp
    to that of the most recent capture-start message.
    """
    for i in range(retries):
        last_target = float(redis_server.get("{}:last-target".format(product_id)))
        last_start = float(redis_server.get("{}:last-capture-start".format(product_id)))
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
        target = redis_server.get(target_key)
    return target 

def main(port, cfg_file, triggermode):
    # Refactor this in future.
    # For further information on the Hashpipe-Redis gateway messages, please 
    # see appendix B in https://arxiv.org/pdf/1906.07391.pdf
    log = set_logger(log_level = logging.DEBUG)
    log.info("Starting Coordinator")
    # Set number of instances and streams per instance
    # based on configuration file.
    try:
        hashpipe_instances, streams_per_instance = configure(cfg_file)
        log.info('Configured from {}'.format(cfg_file))
    except:
        log.warning('Configuration not updated; old configuration might be present.')
    red = redis.StrictRedis(port=port, decode_responses=True)
    ps = red.pubsub(ignore_subscribe_messages=True)
    # Subscribe to the required Redis channels.
    ps.subscribe(ALERTS_CHANNEL)
    ps.subscribe(SENSOR_CHANNEL)
    ps.subscribe(TRIGGER_CHANNEL)
    # Initialise trigger mode (idle, armed or auto)
    red.set('coordinator:trigger_mode', triggermode)
    log.info('Trigger mode on startup: {}'.format(triggermode))
    # Process incoming Redis messages:
    try:
        for message in ps.listen():
            # Split message only twice - the format is as follows:
            # message_type:description:value
            # OR message_type:description (if there is no associated value)
            msg_parts = message['data'].split(':', 2)
            if len(msg_parts) < 2:
                log.info("Not processing this message --> {}".format(message))
                continue
            msg_type = msg_parts[0]
            description = msg_parts[1]
            if(len(msg_parts) > 2):
                value = msg_parts[2]
            # Global channel for all processing nodes
            global_chan = HPGDOMAIN + ':///set'
            # If trigger mode is changed on the fly:
            if((msg_type == 'coordinator') & (description == 'trigger_mode')):
                triggermode = value
                red.set('coordinator:trigger_mode', value)
                log.info('Trigger mode set to \'{}\''.format(value))
            # If all the sensor values required on configure have been
            # successfully fetched by the katportalserver
            if(msg_type == 'conf_complete'):
                product_id = description
                tracking = 0 # Initialise tracking state to 0
                log.info('New subarray built: {}'.format(product_id))
                # Get IP offset (for ingesting fractions of the band)
                try:
                    offset = int(red.get('{}:ip_offset'.format(product_id)))
                    if(offset > 0):
                        log.info('Stream IP offset applied: {}'.format(offset))
                except:
                    log.info("No stream IP offset; defaulting to 0")
                    offset = 0
                # Generate list of stream IP addresses
                all_streams = json.loads(json_str_formatter(red.get(
                    "{}:streams".format(product_id))))
                streams = all_streams[STREAM_TYPE]
                stream_addresses = streams[FENG_TYPE]
                addr_list, port, n_addrs = read_spead_addresses(stream_addresses, 
                    len(hashpipe_instances), 
                    streams_per_instance, offset)
                n_red_chans = len(addr_list)
                # Number of antennas
                ant_key = '{}:antennas'.format(product_id)
                n_ants = len(red.lrange(ant_key, 0, red.llen(ant_key)))
                pub_gateway_msg(red, global_chan, 'NANTS', n_ants, log, True)
                # Sync time (UNIX, seconds)
                sensor_key = cbf_sensor_name(product_id, red, 'sync_time')   
                sync_time = int(float(red.get(sensor_key))) # Is there a cleaner way?
                pub_gateway_msg(red, global_chan, 'SYNCTIME', 
                    sync_time, log, True)
                # Port
                pub_gateway_msg(red, global_chan, 'BINDPORT', port, log, True)
                # Total number of streams
                pub_gateway_msg(red, global_chan, 'FENSTRM', 
                    n_addrs, log, True)
                # Total number of frequency channels    
                n_freq_chans = red.get('{}:n_channels'.format(product_id))
                pub_gateway_msg(red, global_chan, 'FENCHAN', 
                    n_freq_chans, log, True)
                # Number of channels per substream
                sensor_key = cbf_sensor_name(product_id, red, 
                    'antenna_channelised_voltage_n_chans_per_substream')   
                n_chans_per_substream = red.get(sensor_key)
                pub_gateway_msg(red, global_chan, 'HNCHAN', 
                    n_chans_per_substream, log, True)
                # Number of spectra per heap
                sensor_key = cbf_sensor_name(product_id, red, 
                    'tied_array_channelised_voltage_0x_spectra_per_heap')   
                spectra_per_heap = red.get(sensor_key)
                pub_gateway_msg(red, global_chan, 'HNTIME', spectra_per_heap,
                    log, True)
                # Number of ADC samples per heap
                sensor_key = cbf_sensor_name(product_id, red, 
                    'antenna_channelised_voltage_n_samples_between_spectra')   
                adc_per_spectra = red.get(sensor_key)
                adc_per_heap = int(adc_per_spectra)*int(spectra_per_heap)
                pub_gateway_msg(red, global_chan, 'HCLOCKS', 
                    adc_per_heap, log, True)
                # Centre frequency
                sensor_key = stream_sensor_name(product_id, red, 
                    'antenna_channelised_voltage_centre_frequency')
                centre_freq = red.get(sensor_key)
                centre_freq = float(centre_freq)/1e6
                centre_freq = '{0:.17g}'.format(centre_freq)
                pub_gateway_msg(red, global_chan, 'FECENTER', centre_freq, 
                    log, True)
                # Coarse channel bandwidth (from F engines)
                # Note: no sign information!  
                sensor_key = cbf_sensor_name(product_id, red, 
                    'adc_sample_rate')
                adc_sample_rate = red.get(sensor_key)
                coarse_chan_bw = float(adc_sample_rate)/2.0/int(n_freq_chans)/1e6
                coarse_chan_bw = '{0:.17g}'.format(coarse_chan_bw)
                pub_gateway_msg(red, global_chan, 'CHAN_BW', coarse_chan_bw, 
                    log, True) 
                # Set PKTSTART to 0 on configure
                pub_gateway_msg(red, global_chan, 'PKTSTART', 0, log, True) 
                # Subscription IPs for processing nodes
                for i in range(n_red_chans):
                    local_chan = HPGDOMAIN + '://' + hashpipe_instances[i] + '/set'
                    # Number of streams for instance i
                    n_streams_per_instance = int(addr_list[i][-1])+1
                    pub_gateway_msg(red, local_chan, 'NSTRM', 
                        n_streams_per_instance, log, True)
                    # Absolute starting channel for instance i
                    s_chan = offset*int(n_chans_per_substream) + i*n_streams_per_instance*int(n_chans_per_substream)
                    pub_gateway_msg(red, local_chan, 'SCHAN', s_chan, log, True)
                    # Destination IP addresses for instance i
                    pub_gateway_msg(red, local_chan, 'DESTIP', addr_list[i],
                        log, True)
            # If the current subarray is deconfigured:
            if(msg_type == 'deconfigure'):
                pub_gateway_msg(red, global_chan, 'DESTIP', '0.0.0.0', 
                    log, False)
                log.info('Subarray deconfigured')
            # Handle the full data-suspect bitmask, one bit per polarisation
            # per F-engine.
            if(msg_type == 'data-suspect'):
                mask = value
                bitmask = '#{:x}'.format(int(mask, 2))
                pub_gateway_msg(red, global_chan, 'FESTATUS', bitmask, 
                    log, False)
            # If the current subarray is on source and tracking:
            if(msg_type == 'tracking'):
                product_id = description
                if((tracking == 0) & (triggermode != 'idle')):
                    # Publish DATADIR according to the current schedule 
                    # block ID. This entails retrieving the list of 
                    # schedule blocks, extracting the ID of the current
                    # one and formatting it as a file path.
                    # Schedule block IDs follow the format:
                    # YYYYMMDD-XXXX where XXXX is the schedule block number 
                    # (in order of assignment). To create the correct
                    # DATADIR, we format it (per schedule block) as follows:
                    # DATADIR=YYYYMMDD/XXXX
                    # If the schedule block ID is not known, we set it to:
                    # DATADIR=Unknown_SB
                    current_sb_id = 'Unknown_SB' # Default
                    try: 
                        sb_key = '{}:sched_observation_schedule_1'.format(product_id)
                        sb_ids = red.get(sb_key)
                        # First ID in list is the current SB (as per CAM GUI)
                        current_sb_id = sb_ids.split(',')[0] 
                        # Format for file path
                        current_sb_id = current_sb_id.replace('-', '/')
                    except:
                        log.error("Schedule block IDs not available")
                        log.warning("Setting DATADIR=/buf0/Unknown_SB")
                    # Publish DATADIR to gateway
                    datadir = '/buf0/{}'.format(current_sb_id)
                    pub_gateway_msg(red, global_chan, 'DATADIR', datadir, 
                        log, False)
                    # Get new target:
                    ant_key = '{}:antennas'.format(product_id) 
                    ant_list = red.lrange(ant_key, 0, red.llen(ant_key))
                    log.info(ant_list)
                    target_key = "{}:{}_target".format(product_id, ant_list[0])
                    target_str = get_target(product_id, target_key, 5, 15, red, log)
                    target_str, ra_str, dec_str = target_name(target_str, 16, delimiter = "|")
                    # Publish new RA_STR and DEC_STR values to gateway
                    pub_gateway_msg(red, global_chan, 'RA_STR', ra_str, 
                        log, False)
                    pub_gateway_msg(red, global_chan, 'DEC_STR', dec_str, 
                        log, False)
                    # Get target name and publish:
                    src_name = target_str
                    pub_gateway_msg(red, global_chan, 'SRC_NAME', src_name, 
                        log, False)
                    # Set PKTSTART:
                    pkt_idx_start = get_start_idx(red, hashpipe_instances, 
                        PKTIDX_MARGIN, log)
                    pub_gateway_msg(red, global_chan, 'PKTSTART', 
                        pkt_idx_start, log, False)
                    # Alert via slack:
                    slack_message = "{}::meerkat:: New recording started!".format(SLACK_CHANNEL)
                    red.publish(PROXY_CHANNEL, slack_message)
                    # If armed, reset triggermode to idle after triggering 
                    # once.
                    if(triggermode == 'armed'):
                        triggermode = 'idle'
                        red.set('coordinator:trigger_mode', 'idle')
                        log.info('Triggermode set to \'idle\' from \'armed\'')
                    elif('nshot' in triggermode):
                        nshot = triggermode.split(':')
                        n = int(nshot[1]) - 1
                        triggermode = '{}:{}'.format(nshot[0], n)
                        log.info('Triggermode: n shots remaining: {}'.format(n))
                        if(n <= 0):
                            triggermode = 'idle'
                            red.set('coordinator:trigger_mode', 'idle')
                            log.info('Triggermode set to \'idle\' from \'nshot\'')
                # Set state to 'tracking'
                tracking = 1 
            # Update pointing coordinates:
            if('pos_request_base' in description):
                # RA and Dec (in degrees)
                if('dec' in description):
                    dec_deg = value
                    pub_gateway_msg(red, global_chan, 'DEC', dec_deg, 
                        log, False)
                elif('ra' in description):    
                    # pos_request_base_ra value is given in hrs (single float 
                    # value)
                    ra_hrs = value
                    ra_deg = float(ra_hrs)*15.0 # Convert to degrees
                    pub_gateway_msg(red, global_chan, 'RA', ra_deg, 
                        log, False)
                # Azimuth and elevation (in degrees):
                elif('azim' in description):
                    az = value
                    pub_gateway_msg(red, global_chan, 'AZ', az, log, False)
                elif('elev' in description):
                    el = value
                    pub_gateway_msg(red, global_chan, 'EL', el, log, False)
            # When a source is no longer being tracked:  
            if msg_type == 'not-tracking':
                if(tracking == 1):
                    # For the moment during testing, get dwell time from one 
                    # of the hosts. Then set to zero and then back to to the 
                    # original dwell time.
                    host_key = '{}://{}/status'.format(HPGDOMAIN, 
                        hashpipe_instances[2])
                    dwell_time = get_dwell_time(red, host_key)
                    pub_gateway_msg(red, global_chan, 'DWELL', '0', 
                        log, False)
                    pub_gateway_msg(red, global_chan, 'PKTSTART', '0', 
                        log, False)
                    time.sleep(1) # Wait for processing nodes.
                    pub_gateway_msg(red, global_chan, 'DWELL', dwell_time, 
                        log, False)
                tracking = 0
    except KeyboardInterrupt:
        log.info("Stopping coordinator")
        sys.exit(0)
    except Exception as e:
        log.error(e)
        sys.exit(1)

if __name__ == "__main__":
    cli()
