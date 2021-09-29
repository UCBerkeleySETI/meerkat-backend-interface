"""Script to retrieve applied phase solutions from katportal. 

   Needs to be provided with an antenna-based sensor with braces in place of 
   antenna name.

   The proxy name must be included (which means also specifying the subarray 
   index). 

   e.g. 
   cbf_{}_wide_antenna_channelised_voltage_{}h_eq

   In addition, the number of the current active subarray must be specified. 

"""

from tornado import ioloop
from tornado.gen import coroutine
from katportalclient import KATPortalClient
import redis
import csv
import logging
import argparse
import sys
import json
import ast

def cli(args = sys.argv[0]):
    """CLI for antenna sensor retrieval.
    """
    usage = "{} [options]".format(args)
    description = 'Retrieve sensor values for a specified sensor for each antenna in an active subarray.'
    parser = argparse.ArgumentParser(prog = 'applied_phases', usage = usage, 
        description = description)
    parser.add_argument('--pattern', 
        type = str,
        help = 'Sensor name. Replace subarray number and antenna name with braces: {}')
    parser.add_argument('--subarray', 
        type = str,
        help = 'Active subarray number.') 
    parser.add_argument('--outfile', 
        type = str,
        help = 'Output file name.') 
    if(len(sys.argv[1:])==0):
        parser.print_help()
        parser.exit()
    args = parser.parse_args()
    main(sensor_pattern = args.pattern, subarray_number = args.subarray, outfile = args.outfile)

@coroutine
def fetch_sensor_pattern(pattern, client, log):
    """Fetch sensor pattern for each antenna. 
    """
    try:
        sensor_details = yield client.sensor_values(pattern, include_value_ts=True)
        return(sensor_details)
    except Exception as e:
        log.error(e)
        return(None)

def main(sensor_pattern, subarray_number, outfile):
    """Retrieves values for a specific sensor from each antenna in an 
       active subarray.
    """

    LOGGING_FORMAT = "[%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s] %(message)s"
    logging.basicConfig(format=LOGGING_FORMAT)
    log = logging.getLogger('BLUSE')
    log.setLevel(logging.DEBUG)

    # Retrieve CAM address for current subarray:    
    subarray_name = 'array_{}'.format(subarray_number)
    redis_server = redis.StrictRedis(decode_responses = True) 
    cam_url = redis_server.get("{}:{}".format(subarray_name, 'cam:url'))

    # Instantiate client for retrieval of sensor data from CAM:
    client = KATPortalClient(cam_url, on_update_callback=None, logger=log)
    io_loop = ioloop.IOLoop.current()

    # Fetch list of antennas associated with current subarray:
    ant_sensor = 'cbf_{}_receptors'.format(subarray_number)
    ant_details = io_loop.run_sync(lambda: fetch_sensor_pattern(ant_sensor, client, log))
    ant_list = []
    for sensor, details in ant_details.items():
        ant_list = details.value 
    if(len(ant_list) == 0):
        log.error('No antennas found in subarray {}. Note assuming CBF component is CBF and not CBF_dev'.format(subarray_number))
        sys.exit('Aborting')
    ant_list = ant_list.split(',') 

    # Build and retrieve specified sensor data from each antenna:
    all_ant_output = []
    for ant in ant_list:
        sensor_pattern = sensor_pattern.format(subarray_number, ant)
        ant_i_sensor = io_loop.run_sync(lambda: fetch_sensor_pattern(sensor_pattern, client, log))
        for sensor, details in ant_i_sensor.items():
            sensor_vals = details.value
            sensor_vals = ast.literal_eval(sensor_vals)
            # Convert complex numbers to str for json format
            sensor_vals = list(map(str, sensor_vals))
            all_ant_output.append([sensor, sensor_vals])
        log.info('Results for {} retrieved'.format(ant))
    log.info('Saving output...')
    with open('{}.json'.format(outfile), 'w') as f:
        json.dumps(all_ant_output, f)

if(__name__ == '__main__'):
    cli()
