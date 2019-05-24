#!/usr/bin/env python

from optparse import OptionParser
import json
import logging
import sys
import redis
from meerkat_backend_interface import redis_tools
from meerkat_backend_interface.logger import log

CHANNEL     = redis_tools.REDIS_CHANNELS.alerts  # Redis channel to listen on
STREAM_TYPE = 'cbf.antenna_channelised_voltage'  # Type of stream to distribute
NCHANNELS   = 64                                 # Number of channels to distribute into
CHANNELS = ["chan{:03d}".format(n) for n in range(NCHANNELS)]

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

def create_ip_list(addr0, n_addrs):
    """Creates list of IP multicast subscription addresses.

    Args:
        addr0 (str): first IP address in the list.
        n_addrs (int): number of consecutive IP addresses for subscription.

    Returns:
        addr_list (list): list of IP addresses for subscription.
    """
    prefix, suffix0 = addr0.rsplit('.', 1)
    addr_list = [addr0]
    for i in range(1, n_addrs):
        addr_list.append(prefix + '.{}'.format(i + int(suffix0)))
    return addr_list

def parse_spead_addresses(spead_addrs):
    """Parses spead addresses given in the format: spead://<ip>[+<count>]:<port>
    Assumes this format.

    Args:
        spead_addrs (str): string containing spead IP addresses in the format above.

    Returns:
        addr_list (list): list of spead stream IP addresses for subscription.
        port (int): port number.
    """
    addrs = spead_addrs.split('/')[-1]
    addrs, port = addrs.split(':')
    try:
        addr0, n_addrs = addrs.split('+')
        n_addrs = int(n_addrs) + 1
        addr_list = create_ip_list(addr0, n_addrs)
    except ValueError:
        n_addrs = 1
        addr_list = [addrs]
    return addr_list, port

def cli():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-p', '--port', dest='port', type=long,
                      help='Redis port to connect to', default=6379)
    (opts, args) = parser.parse_args()
    # if not opts.port:
    #     print "MissingArgument: Port number"
    #     sys.exit(-1)
    main(port=opts.port)

def main(port):
    FORMAT = "[ %(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    # logger = logging.getLogger('reynard')
    logging.basicConfig(format=FORMAT)
    log.setLevel(logging.DEBUG)
    log.info("Starting distributor")
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
                all_streams = json.loads(json_str_formatter(red.get("{}:streams".format(product_id))))
                streams = all_streams[STREAM_TYPE]
                addr_list, port = parse_spead_addresses(streams.values()[0])
                nstreams = len(addr_list)
                if nstreams > NCHANNELS:
                    log.warning("More than {} ({}) stream addresses found".format(NCHANNELS, nstreams))
                for i in range(min(nstreams, NCHANNELS)):
                    msg = "configure:{}:stream:{}:{}".format(product_id, addr_list[i], port)
                    red.publish(CHANNELS[i], msg)
    except KeyboardInterrupt:
        log.info("Stopping distributor")
        sys.exit(0)
    except Exception as e:
        log.error(e)
        sys.exit(1)

if __name__ == "__main__":
    cli()
