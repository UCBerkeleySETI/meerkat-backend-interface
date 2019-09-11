#!/usr/bin/env python

from optparse import OptionParser
import signal
import sys
import logging

from meerkat_backend_interface.katportal_server import BLKATPortalClient
from meerkat_backend_interface.logger import log, set_logger

def cli(prog = sys.argv[0]):
    usage = 'usage: %prog [options]'
    parser = OptionParser(usage = usage)
    parser.add_option('-c', '--config', type = str,
                     help = 'Config filename (yaml)', default = 'config.yml')
    (opts, args) = parser.parse_args()
    main(config = opts.config)

def on_shutdown():
    # TODO: uncomment when you deploy
    # notify_slack("KATPortal module at MeerKAT has halted. Might want to check that!")
    log.info("Shutting Down Katportal Clients")
    sys.exit()

def main(config):
    log = set_logger(log_level = logging.DEBUG)
    log.info("Starting Katportal Client")
    client = BLKATPortalClient(config)
    signal.signal(signal.SIGINT, lambda sig, frame: on_shutdown())
    client.start()

if __name__ == '__main__':
    cli()
