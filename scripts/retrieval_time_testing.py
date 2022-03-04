"""Test and compare retrieval times for different katportal sensor retrieval
methods.

Retrieval is now possible per component (e.g. 'cbf_1', 'sdp_1', etc). This 
speeds up retrieval time. 
"""

from katportalclient import KATPortalClient

import time
import argparse 
import sys
from tornado import ioloop
from tornado.gen import coroutine

def cli(args = sys.argv[0]):
    """CLI for retrieval testing.
    """
    usage = '{} [options]'.format(args)
    description = 'Test sensor retrieval time by retrieval method'
    parser = argparse.ArgumentParser(prog = 'test_retrieval_time',
                                     usage = usage,
                                     description = description)
    parser.add_argument('--ip',
                        type = str,
                        default = '127.0.0.1',
                        help = 'CAM ip address')
    parser.add_argument('--sensor',
                        type = str,
                        default = 'cbf_1_target',
                        help = 'Full sensor string (incl. prefix and component)')
    parser.add_argument('--component',
                        type = str,
                        default = 'cbf_1',
                        help = 'Component to which sensor belongs')
    if(len(sys.argv[1:]) == 0):
        parser.print_help()
        parser.exit()
    args = parser.parse_args()
    main(ip = args.ip,
         sensor = args.sensor,
         component = args.component)

@coroutine
def sensor_direct(client, sensor_string):
    """Retrieve sensor values via the original method. 

    Args:
        
        sensor_string (str): full sensor name including prefixes (if any). 

    Returns:

        request_time (float): UNIX time at which sensor request was made. 
    """
    try:
        request_time = time.time()
        sensor_details = yield client.sensor_values(sensor_string,
            include_value_ts=True)
        #print(sensor_details)
    except:
        print('Could not retrieve sensor.')
        request_time = 0
    return request_time

@coroutine
def sensor_by_component(client, sensor_string, components):
    """Retrieve sensor values per component (has shown better performance). 

    Args:
    
        sensor_string (str): full sensor name including prefixes (if any). 
        components (list): list of components from which to request sensor. 

    Returns:

        request_time (float): UNIX time at which sensor request was made. 

    """
    try:
        request_time = time.time()
        sensor_details = yield client.sensor_values(sensor_string, components,
            include_value_ts=True)
        #print(sensor_details)
    except:
        print('Could not retrieve sensor.')
        request_time = 0
    return request_time


def main(ip, sensor, component):
    """Comparison of sensor request duration. 

    Args:

        ip (str): CAM ip address. 
        sensor (str): full sensor name including component prefixes. 
        component (str): component to which sensor belongs. 

    Returns:

        None  
    """
    cam_url = 'http://{}/api/client/'.format(ip)
    client = KATPortalClient(cam_url, on_update_callback=None)
    io_loop = ioloop.IOLoop.current()
 
    for i in range(0, 1):
        print('Fetching sensor details\n')      
        print('Direct Retrieval:') 
        retrieval_ts = io_loop.run_sync(lambda: sensor_direct(client, sensor))
        retrieval_d = time.time() - retrieval_ts
        print('Retrieval time: {}'.format(retrieval_d))
        print('\nRetrieval by component:')
        retrieval_ts = io_loop.run_sync(lambda: sensor_by_component(client, sensor, [component]))
        retrieval_c = time.time() - retrieval_ts
        print('Retrieval time: {}'.format(time.time() - retrieval_ts))
        print('\nRatio: {}'.format(retrieval_d/retrieval_c))

if(__name__ == '__main__'):
    cli()
