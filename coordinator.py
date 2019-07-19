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

def create_addr_list(addr0, n_groups, n_addrs):
    """Creates list of IP multicast subscription address groups.

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

def read_spead_addresses(spead_addrs, n_groups):
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
        addr_list = create_addr_list(addr0, n_groups, n_addrs)
    except ValueError:
        addr_list = [addrs + '+0']
    return addr_list, port

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
                return(cfg['hashpipe_instances'], cfg['sensors_once_off'], 
                cfg['sensors_subscribe'])
            except yaml.YAMLError as E:
                log.error(E)
    except IOError:
        log.error('Config file not found')

def main(port, cfg_file):
    FORMAT = "[ %(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    logging.basicConfig(format=FORMAT)
    log.setLevel(logging.DEBUG)
    log.info("Starting coordinator")
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
            if msg_type == 'configure':
                try:
                    hashpipe_instances, sensors_once_off, sensors_subscribe = configure(cfg_file)           
                except:
                    log.warning('Configuration not updated; old configuration might be present.')
                all_streams = json.loads(json_str_formatter(red.get("{}:streams".format(product_id))))
                streams = all_streams[STREAM_TYPE]
                addr_list, port = read_spead_addresses(streams.values()[0], len(hashpipe_instances))
                nchannels = len(addr_list)
                for i in range(nchannels):
                    msg = 'DESTIP={}'.format(addr_list[i])
                    channel = HPGDOMAIN + '://' + hashpipe_instances[i] + '/set'
                    red.publish(channel, msg)
                red.publish(HPGDOMAIN + ':///set', 'BINDPORT=' + port)
            if msg_type == 'deconfigure':
                channel = HPGDOMAIN + ':///set'
                red.publish(channel, 'DESTIP=0.0.0.0')
            if msg_type == 'capture-start':
                channel = HPGDOMAIN + ':///set'
                red.publish(channel, 'NETSTAT=RECORD')
            if msg_type == 'capture-stop':
                channel = HPGDOMAIN + ':///set'
                red.publish(channel, 'NETSTAT=LISTEN')
    except KeyboardInterrupt:
        log.info("Stopping coordinator")
        sys.exit(0)
    except Exception as e:
        log.error(e)
        sys.exit(1)

if __name__ == "__main__":
    cli()
