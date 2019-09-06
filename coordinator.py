#!/usr/bin/env python

from optparse import OptionParser
import yaml
import json
import logging
import sys
import redis
import numpy as np
from meerkat_backend_interface import redis_tools
from meerkat_backend_interface.logger import log

CHANNEL     = redis_tools.REDIS_CHANNELS.alerts  # Redis channel to listen on
STREAM_TYPE = 'cbf.antenna_channelised_voltage'  # Type of stream to distribute
HPGDOMAIN   = 'bluse'

def json_str_formatter(str_dict):
    """Formatting for json.loads

    Args:
        str_dict (str): str containing dict of spead streams (received on ?configure).

    Returns:
        str_dict (str): str containing dict of spead streams, formatted for use with json.loads
    """
    # Is there a better way of doing this?
    str_dict = str_dict.replace('\'', '"')  # Swap quote types for json format
    str_dict = str_dict.replace('u', '')  # Remove unicode 'u'
    return str_dict

def create_addr_list_filled(addr0, n_groups, n_addrs, streams_per_instance):
    """Creates list of IP multicast subscription address groups.
    Fills the list for each available processing instance 
    sequentially untill all streams have been assigned.
    """
    prefix, suffix0 = addr0.rsplit('.', 1)
    addr_list = []
    if(n_addrs > streams_per_instance*n_groups):
        log.warning('Too many streams: {} will not be processed.'.format(n_addrs - streams_per_instance*n_groups))
        for i in range(0, n_groups):
            addr_list.append(prefix + '.{}+{}'.format(int(suffix0), streams_per_instance - 1))
            suffix0 = int(suffix0) + streams_per_instance
    else:
        n_instances_req = int(np.ceil(n_addrs/float(streams_per_instance)))
        for i in range(1, n_instances_req):
            addr_list.append(prefix + '.{}+{}'.format(int(suffix0), streams_per_instance - 1))
            suffix0 = int(suffix0) + streams_per_instance
        addr_list.append(prefix + '.{}+{}'.format(int(suffix0), n_addrs - 1 - i*streams_per_instance))
    return addr_list

def create_addr_list_distributed(addr0, n_groups, n_addrs):
    """Creates list of IP multicast subscription address groups.
    Attempts to divide the number of streams equally by the number
    of available processing instances.     

    Args:
        addr0 (str): first IP address in the list.
        n_groups (int): number of available hashpipe instances.
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
        addr_list.append(prefix + '.{}'.format(int(suffix0)) + '+' + str(n_per_group[i]-1))
        suffix0 = int(suffix0) + n_per_group[i]
    return addr_list

def read_spead_addresses(spead_addrs, n_groups, streams_per_instance):
    """Parses spead addresses given in the format: spead://<ip>+<count>:<port>
    Assumes this format.

    Args:
        spead_addrs (str): string containing spead IP addresses in the format above.
        n_groups (int): number of stream addresses to be sent to each processing instance.

    Returns:
        addr_list (list): list of spead stream IP address groups.
        port (int): port number.
    """
    addrs = spead_addrs.split('/')[-1]
    addrs, port = addrs.split(':')
    try:
        addr0, n_addrs = addrs.split('+')
        n_addrs = int(n_addrs) + 1
        addr_list = create_addr_list_filled(addr0, n_groups, n_addrs, streams_per_instance)
    except ValueError:
        addr_list = [addrs + '+0']
        n_addrs = 1
    return addr_list, port, n_addrs

def cli():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-p', '--port', dest='port', type=long,
                      help='Redis port to connect to', default=6379)
    parser.add_option('-c', '--config', dest='cfg_file', type=str,
                      help='Config filename (yaml)', default = 'config.yml')
    (opts, args) = parser.parse_args()
    # if not opts.port:
    #     print "MissingArgument: Port number"
    #     sys.exit(-1)
    main(port=opts.port, cfg_file=opts.cfg_file)

def configure(cfg_file):
    try:
        with open(cfg_file, 'r') as f:
            try:
                cfg = yaml.safe_load(f)
                return(cfg['hashpipe_instances'], cfg['streams_per_instance'][0])
            except yaml.YAMLError as E:
                log.error(E)
    except IOError:
        log.error('Config file not found')

def main(port, cfg_file):
    FORMAT = "[ %(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    logging.basicConfig(format=FORMAT)
    log.setLevel(logging.DEBUG)
    log.info("Starting coordinator")
    try:
        hashpipe_instances, streams_per_instance = configure(cfg_file)
        log.info('Configured from {}'.format(cfg_file))
    except:
        log.warning('Configuration not updated; old configuration might be present.')
    red = redis.StrictRedis(port=port)
    ps = red.pubsub(ignore_subscribe_messages=True)
    ps.subscribe(CHANNEL)
    try:
        for message in ps.listen():
            msg_parts = message['data'].split(':')
            if len(msg_parts) != 2:
                log.info("Not processing this message --> {}".format(message))
                continue
            msg_type = msg_parts[0]
            product_id = msg_parts[1]
            if msg_type == 'conf_complete':
                all_streams = json.loads(json_str_formatter(red.get("{}:streams".format(product_id))))
                streams = all_streams[STREAM_TYPE]
                addr_list, port, n_addrs = read_spead_addresses(streams.values()[0], len(hashpipe_instances), streams_per_instance)
                n_red_chans = len(addr_list)
                # Number of antennas
                ant_key = '{}:antennas'.format(product_id)
                n_ants = len(red.lrange(ant_key, 0, red.llen(ant_key)))
                red.publish(HPGDOMAIN + ':///set', 'NANTS=' + str(n_ants)) 
                # Sync time (UNIX, seconds)
                sync_key = '{}:cbf_{}_wide_sync_time'.format(product_id, product_id[-1])
                sync_time =  red.get(sync_key)
                red.publish(HPGDOMAIN + ':///set', 'SYNCTIME=' + sync_time)
                # Port
                red.publish(HPGDOMAIN + ':///set', 'BINDPORT=' + port)
                # Total number of streams
                red.publish(HPGDOMAIN + ':///set', 'FENSTRM=' + str(n_addrs))
                # Total number of frequency channels    
                n_freq_channels = red.get('{}:n_channels'.format(product_id))
                red.publish(HPGDOMAIN + ':///set', 'FENCHAN=' + n_freq_channels)
                # Number of channels per substream
                n_chans_per_substream = red.get('{}:cbf_{}_wide_antenna_channelised_voltage_n_chans_per_substream'.format(product_id, product_id[-1]))
                red.publish(HPGDOMAIN + ':///set', 'HNCHAN=' + n_chans_per_substream)
                # Number of spectra per heap
                spectra_per_heap = red.get('{}:cbf_{}_wide_tied_array_channelised_voltage_0x_spectra_per_heap'.format(product_id, product_id[-1]))
                red.publish(HPGDOMAIN + ':///set', 'HNTIME=' + spectra_per_heap)
                # Number of ADC samples per heap
                adc_per_spectra = red.get('{}:cbf_{}_wide_antenna_channelised_voltage_n_samples_between_spectra'.format(product_id, product_id[-1]))
                adc_per_heap = int(adc_per_spectra)*int(spectra_per_heap)
                red.publish(HPGDOMAIN + ':///set', 'HCLOCKS=' + str(adc_per_heap))

                for i in range(n_red_chans):
                    red_channel = HPGDOMAIN + '://' + hashpipe_instances[i] + '/set'
                    # Destination IP addresses for instance i
                    msg = 'DESTIP={}'.format(addr_list[i])
                    red.publish(red_channel, msg)
                    n_streams_per_instance = int(addr_list[i][-1])+1
                    # Number of streams for instance i
                    msg = 'NSTRM={}'.format(n_streams_per_instance)
                    red.publish(red_channel, msg)
                    # Number of channels dealt with by instance i
                    msg = 'NCHAN={}'.format(n_streams_per_instance*int(n_chans_per_substream))
                    red.publish(red_channel, msg)
                    # Absolute starting channel for instance i
                    s_chan = i*n_streams_per_instance*int(n_chans_per_substream)
                    red.publish(red_channel, 'SCHAN=' + str(s_chan))
                          
            if msg_type == 'deconfigure':
                red_channel = HPGDOMAIN + ':///set'
                red.publish(red_channel, 'DESTIP=0.0.0.0')
            if msg_type == 'capture-start':
                red_channel = HPGDOMAIN + ':///set'
                red.publish(red_channel, 'NETSTAT=RECORD')
            if msg_type == 'capture-stop':
                red_channel = HPGDOMAIN + ':///set'
                red.publish(red_channel, 'NETSTAT=LISTEN')
    except KeyboardInterrupt:
        log.info("Stopping coordinator")
        sys.exit(0)
    except Exception as e:
        log.error(e)
        sys.exit(1)

if __name__ == "__main__":
    cli()
