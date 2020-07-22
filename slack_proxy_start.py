from argparse import (
    ArgumentParser,
    ArgumentDefaultsHelpFormatter)

import logging
import sys
import os

from meerkat_backend_interface.logger import set_logger
from meerkat_backend_interface.slack_proxy import SlackProxy

def cli(prog = sys.argv[0]):
    """CLI for Slack proxy.
    """
    usage = "{} [options]".format(prog)
    description = 'Start the Slack proxy'
    parser = ArgumentParser(usage = usage,
                            description = description,
                            formatter_class = ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-p', '--port',
        type = int,
        default = 6379,
        help = 'Port for the Redis server')
    parser.add_argument(
        '-c', '--channel',
        type = str,
        default = 'test_channel',
        help = 'Redis pub/sub channel to listen on')
    parser.add_argument(
        '-d', '--debug',
        action = 'store_true',
        help = 'Set log level to DEBUG')
    args = parser.parse_args()
    main(port = args.port, channel = args.channel, debug = args.debug )

def main(port, channel, debug):
    """Set up and start the Slack proxy.
    """
    if(debug):
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    log = set_logger(log_level = log_level)
    token = os.environ["SLACK_API_TOKEN"]
    slack_proxy = SlackProxy(port, channel, token) 
    slack_proxy.start()

if(__name__ == '__main__'):
    cli()
