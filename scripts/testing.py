#!/usr/bin/env python

"""Script to test certain functionality as best as possible when the 
proper dev environment (devnmk / monctl) is unavailable.
""" 

import logging
from meerkat_backend_interface.coordinator import Coordinator
from meerkat_backend_interface.logger import log, set_logger

if(__name__ == '__main__'):

    log = set_logger(log_level = logging.DEBUG)    

    log.info("Starting tests")

    coord = Coordinator(6379, 'config.yml', 'armed')

    test_1 = 'J1445+0958 | OQ172, radec gaincal, 14:45:16.47, 9:58:36.1'
    test_2 = 'radec gaincal, 14:45:16.47, 9:58:36.1'
    test_3 = 'test, 14:45:16.47, 9:58:36.1'
    
    log.info("Testing input: {}".format(test_1))
    log.info(coord.target_name(test_1, 16, delimiter = "|"))
    
    log.info("Testing input: {}".format(test_2))
    log.info(coord.target_name(test_2, 16, delimiter = "|"))
    
    log.info("Testing input: {}".format(test_3))
    log.info(coord.target_name(test_3, 16, delimiter = "|"))
