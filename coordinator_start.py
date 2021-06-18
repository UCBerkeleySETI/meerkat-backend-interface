#!/usr/bin/env python

from optparse import OptionParser
import signal
import sys
import logging

from meerkat_backend_interface.coordinator import Coordinator
from meerkat_backend_interface.logger import log, set_logger

def cli(prog = sys.argv[0]):
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
                      default = 'idle')
    (opts, args) = parser.parse_args()
    main(port=opts.port, cfg_file=opts.cfg_file, triggermode=opts.triggermode)

def on_shutdown():
    log.info("Coordinator shutting down.")
    sys.exit()

def main(port, cfg_file, triggermode):
    log = set_logger(log_level = logging.DEBUG)
    log.info("Starting Coordinator")
    coord = Coordinator(port, cfg_file, triggermode)
    signal.signal(signal.SIGINT, lambda sig, frame: on_shutdown())
    coord.start()

if(__name__ == '__main__'):
    cli()
