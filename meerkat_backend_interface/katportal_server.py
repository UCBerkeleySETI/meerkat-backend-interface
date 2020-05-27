from __future__ import print_function

import tornado.gen
import uuid
from katportalclient import KATPortalClient
from katportalclient.client import SensorNotFoundError
import redis
from functools import partial
import json
import yaml
import os
import sys
import ast
import json
import numpy as np
from datetime import datetime

from .redis_tools import (
    REDIS_CHANNELS,
    write_pair_redis,
    write_list_redis,
    publish_to_redis
    )
    
from .logger import log as logger

class BLKATPortalClient(object):
    """Client server to KATPortal. Once initialized, the client creates 
    a Tornado ioloop and a connection to the local Redis server.

    Examples:
        >>> client = BLKATPortalClient()
        >>> client.start()

    When start() is called, a loop starts that subscribes to the 'alerts'
    channel of the Redis server. Depending on the message received, various
    processes are run. These include:
        1. Creating a new KATPortalClient object specific to the
            product id just received in a ?configure request.
        2. Querying for schedule block information when ?capture-init is
            received and publishing this to Redis.
        3. Querying for target information when ?capture-start is
            received and publishing this to Redis.
        4. Deleting the corresponding KATPortalClient object when
            a ?deconfigure request is sent.

    TODO:
        1. Support thread-safe stopping of ioloop
    """

    VERSION = 1.0

    def __init__(self, config_file):
        """Our client server to the Katportal"""
        self.redis_server = redis.StrictRedis(decode_responses = True)
        self.p = self.redis_server.pubsub(ignore_subscribe_messages=True)
        self.io_loop = io_loop = tornado.ioloop.IOLoop.current()
        self.subarray_katportals = dict()  # indexed by product IDs
        self.config_file = config_file
        self.ant_sensors = []  # sensors required from each antenna
        self.stream_sensors = []  # stream sensors (for continuous update)
        self.cbf_conf_sensors = []  # cbf sensors to be queried once-off on configure
        self.cbf_sensors = [] # cbf sensors (for continuous update)
        self.stream_conf_sensors = [] # stream sensors for acquisition on configure.
        self.conf_sensors = [] # other sensors to be queried once-off on configure
        self.subarray_sensors = [] # subarray-level sensors
        self.cont_update_sensors = [] # will contain all sensors for continuous update
        self.config_file = config_file #check if needed
        self.cbf_name = 'cbf_1' # Default CBF short name

    def MSG_TO_FUNCTION(self, msg_type):
        MSG_TO_FUNCTION_DICT = {
            'configure'    : self._configure,
            'capture-init' : self._capture_init,
            'capture-start': self._capture_start,
            'capture-stop' : self._capture_stop,
            'capture-done' : self._capture_done,
            'deconfigure'  : self._deconfigure,
            'conf_complete' : self._conf_complete
        }
        return MSG_TO_FUNCTION_DICT.get(msg_type, self._other)
    
    def start(self):
        self.p.subscribe(REDIS_CHANNELS.alerts)
        if(sys.stdout.isatty()):
            self._print_start_image()
        for msg in self.p.listen():
            msg_data = msg['data']
            msg_parts = msg_data.split(':')
            if len(msg_parts) != 2:
                logger.info("Not processing this message --> {}".format(message))
                continue
            msg_type = msg_parts[0]
            product_id = msg_parts[1]
            self.MSG_TO_FUNCTION(msg_type)(product_id)

    def on_update_callback_fn(self, product_id, msg):
        """Handler for messages published over sensor websockets.
        The received sensor values are stored in the redis database.

        Args:
            product_id (str): the product id given in the ?configure request
            msg (dict): a dictionary containing the updated sensor information

        Returns:
            None
        """
        for key, value in msg.items():
            if key == 'msg_data':
                sensor_name = msg['msg_data']['name']
                sensor_value = msg['msg_data']['value']
                # Write sensors (continuous update)
                key = "{}:{}".format(product_id, sensor_name)
                write_pair_redis(self.redis_server, key, repr(sensor_value))
                # Data-suspect mask for publication
                if('data_suspect' in sensor_name):
                    # Make sure sensor value is trustworthy
                    # as these particular sensors take extra time 
                    # to initialise. 
                    # Note: assumes only one data-suspect sensor is 
                    # to be used.
                    sensor_status = msg['msg_data']['status']
                    if(sensor_status == 'nominal'):
                        publish_to_redis(self.redis_server, 
                        REDIS_CHANNELS.sensor_alerts, 
                        '{}:{}:{}'.format('data-suspect', product_id, sensor_value))
                #RA/Dec/Az/El
                elif('pos_request_base' in sensor_name):
                    publish_to_redis(self.redis_server, REDIS_CHANNELS.sensor_alerts,
                    '{}:{}:{}'.format(product_id, sensor_name, sensor_value))
                    # Aternate method:
                    # self.antenna_consensus(product_id, 'pos_request_base_dec')
                # Check for noise diode operation:
                elif('diode' in sensor_name):
                    publish_to_redis(self.redis_server, REDIS_CHANNELS.sensor_alerts,
                    '{}:{}:{}'.format(product_id, sensor_name, sensor_value))
                # Target information for publication
                elif('target' in sensor_name):
                    #self.antenna_consensus(product_id, 'target')
                    publish_to_redis(self.redis_server, REDIS_CHANNELS.sensor_alerts,
                    '{}:{}:{}'.format(product_id, sensor_name, sensor_value))
                    self.save_history(self.redis_server, product_id, 'target', sensor_value)
                # Observation state for publication
                elif('activity' in sensor_name):
                    if(sensor_value == 'track'):
                        publish_to_redis(self.redis_server, 
                        REDIS_CHANNELS.sensor_alerts, 
                        '{}:{}'.format('tracking', product_id))
                    else:
                        publish_to_redis(self.redis_server, 
                        REDIS_CHANNELS.sensor_alerts, 
                        '{}:{}'.format('not-tracking', product_id))
                    # Temporary solution for websocket subscription issue
                    if(sensor_value == 'stop'):
                        sensors_for_update = self.build_sub_sensors(product_id)
                        if(len(sensors_for_update) > 0):
                            self.subscription('unsubscribe', product_id, sensors_for_update)
                        self.io_loop.stop()
                                         
    def subarray_data_suspect(self, product_id):
        """Publish a global subarray data-suspect value by checking each
        individual antenna. If any antennas are marked faulty by an operator, 
        they are logged, but the subarray data-suspect remains False 
        so that the observation may continue with the remaining antennas.

        Args:
            product_id (str): Name of the current subarray.

        Returns:
            None
        """
        ant_key = '{}:antennas'.format(product_id)
        ant_list = self.redis_server.lrange(ant_key, 0, self.redis_server.llen(ant_key))          
        ant_status = []
        try:
            for i in range(len(ant_list)):
                data_suspect = ast.literal_eval(self.redis_server.get('{}:{}_data_suspect'.format(product_id, ant_list[i])))
                marked_faulty_key = '{}:{}_marked_faulty'.format(product_id, ant_list[i])
                marked_faulty = ast.literal_eval(self.redis_server.get(marked_faulty_key))
                if(data_suspect & marked_faulty):
                    # If an antenna is marked faulty while subarray built, allow it and log. 
                    publish_to_redis(self.redis_server, REDIS_CHANNELS.sensor_alerts, marked_faulty_key)
                    ant_status.append(False) # Note that if marked_faulty is True, data_suspect is by definition True. 
                else:
                    ant_status.append(data_suspect)
            if(sum(ant_status) == 0): # all antennas show good data
	            publish_to_redis(self.redis_server, REDIS_CHANNELS.sensor_alerts, '{}:data_suspect:{}'.format(product_id, False))
            else: 
                publish_to_redis(self.redis_server, REDIS_CHANNELS.sensor_alerts, '{}:data_suspect:{}'.format(product_id, True))
        except:
            # If any of the sensors are not available, set subarray data suspect flag to True
            publish_to_redis(self.redis_server, REDIS_CHANNELS.sensor_alerts, '{}:data_suspect:{}'.format(product_id, True))

    def subarray_consensus(self, product_id, sensor, value, components, n_stragglers):
        """Determine if a particular sensor value is the same as a specified 
        value for all of the specified components in a particular subarray, with 
        the exception of a number of stragglers.

        Args:
            product_id (str): Name of the current subarray.
            sensor (str): Sensor to be checked for each component. Note that
            only per-component sensors can be specified.
            value (str): The desired value of all the components.
            components (list): List of components (str).
            n_stragglers (int): The number of components for which a consensus
            is still accepted. 

        Returns:
            consensus (bool): If a subarray consensus has been reached
            or not, subject to the n_stragglers condition.
            mask (str): Mask of which components exhibit the desired value.
            0 is true, 1 is false. 
        """
        mask = np.ones(len(components))
        consensus = False
        sensor_list = ['{}:{}_{}'.format(product_id, component, sensor) for component in components]
        components_status = self.redis_server.mget(sensor_list)
        for i, status in enumerate(components_status):
            if(status == value):
                mask[i] = 0
        if(np.sum(mask) <= n_stragglers):
            consensus = True
        else:
            logger.info('Consensus for {} not reached'.format(sensor))
        return consensus, mask

    def antenna_consensus(self, product_id, sensor_name):
        """Determine if a particular sensor value is the same for all antennas
        in a particular subarray.

        Args:
            product_id (str): Name of the current subarray.
            sensor_name (str): Sensor to be checked for each antenna. Note that
            only per-antenna sensors can be specified.

        Returns:
            None
        """
        ant_key = '{}:antennas'.format(product_id)
        ant_list = self.redis_server.lrange(ant_key, 0, self.redis_server.llen(ant_key))
        ant_status = ''
        ant_compare = ''
        try:
            for i in range(len(ant_list)):
                ant_status = ant_status + self.redis_server.get('{}:{}_{}'.format(product_id, ant_list[i], sensor_name))  
            ant_compare = ant_compare + self.redis_server.get('{}:{}_{}'.format(product_id, ant_list[0], sensor_name))*len(ant_list)
            if(ant_status == ant_compare): # all antennas show the same value
                # Get value from last antenna
                value = ast.literal_eval(self.redis_server.get('{}:{}_{}'.format(product_id, ant_list[i], sensor_name)))
                if(value is not None):
                    publish_to_redis(self.redis_server, REDIS_CHANNELS.sensor_alerts, 
                    '{}:{}:{}'.format(product_id, sensor_name, value))
                    key = '{}:{}'.format(product_id, sensor_name)
                    write_pair_redis(self.redis_server, key, value)
                else:
                    publish_to_redis(self.redis_server, REDIS_CHANNELS.sensor_alerts, 
                    '{}:{}:unavailable'.format(product_id, sensor_name))
            else:
                logger.warning("Antennas do not show consensus for sensor: {}".format(sensor_name))
        except:
            # If any of the sensors are not available:
            publish_to_redis(self.redis_server, REDIS_CHANNELS.sensor_alerts, '{}:{}:unavailable'.format(product_id, sensor_name))

    def gen_ant_sensor_list(self, product_id, ant_sensors):
        """Automatically builds a list of sensor names for each antenna.

        Args:
            product_id (str): the product id given in the ?configure request
            ant_sensors (list): the sensors to be queried for each antenna

        Returns:
            ant_sensor_list (list): the full sensor names associated with each antenna
        """
        ant_sensor_list = []
        # Add sensors specific to antenna components for each antenna:
        ant_key = '{}:antennas'.format(product_id)
        ant_list = self.redis_server.lrange(ant_key, 0, self.redis_server.llen(ant_key))  # list of antennas
        for ant in ant_list:
            for sensor in ant_sensors:
                ant_sensor_list.append(ant + '_' + sensor)
        return ant_sensor_list
 
    def gen_stream_sensor_list(self, product_id, stream_sensors, cbf_prefix):
        """Automatically builds a list of stream sensor names.

        Args:
            product_id (str): The product id given in the ?configure request.
            stream_sensors (list): The stream sensors to be subscribed to.
            cbf_prefix (str): full CBF prefix. 

        Returns:
            stream_sensor_list (list): the full sensor names.
        """
        stream_sensor_list = []
        for sensor in stream_sensors:
            sensor_name = 'subarray_{}_streams_{}_{}'.format(product_id[-1], cbf_prefix, sensor)
            stream_sensor_list.append(sensor_name)
        return stream_sensor_list

    def gen_cbf_sensor_list(self, cbf_sensors, cbf_name):
        """Builds sensor list for cbf sensor names.

        Args:
            cbf_sensors (list): The cbf sensors to be subscribed to.
            cbf_name (str): short CBF name. 

        Returns:
            cbf_sensor_list (list): the full sensor names.
        """
        cbf_sensor_list = []
        for sensor in cbf_sensors:
            sensor_name = '{}_{}'.format(cbf_name,  sensor)
            cbf_sensor_list.append(sensor_name)
        return cbf_sensor_list

    def configure_katportal(self, cfg_file):
        """Configure the katportal_server from the .yml config file.
      
        Args:
            cfg_file (str): File path to .yml config file.
      
        Returns:
            None
        """
        try:
            with open(cfg_file, 'r') as f:
                try:
                    cfg = yaml.safe_load(f)
                    return(cfg['sensors_per_antenna'], cfg['cbf_sensors_on_configure'],           
                    cfg['stream_sensors'], cfg['cbf_sensors'], cfg['sensors_on_configure'],
                    cfg['array_sensors'], cfg['stream_sensors_on_configure'])
                except yaml.YAMLError as E:
                    logger.error(E)
        except IOError:
            logger.error('Config file not found')

    @tornado.gen.coroutine
    def subscription(self, sub, product_id, sensor_list):
        """Subscribes or unsubscribes to each sensor listed for asynchronous 
           updates.
        
        Args:
            sub (str): Subscribe or unsubscribe. 
            sensor_list (list): Full names of sensors to subscribe to.
            product_id (str): The product ID given in the ?configure request.

        Returns:
            None
        """
        yield self.subarray_katportals[product_id].connect()
        if(sub == 'subscribe'):
            result = yield self.subarray_katportals[product_id].subscribe(namespace = product_id) 
            for sensor in sensor_list:
                # Using product_id as namespace (unique to each subarray)
                result = yield self.subarray_katportals[product_id].set_sampling_strategies(product_id, sensor, 'event')
        elif(sub == 'unsubscribe'):
            result = yield self.subarray_katportals[product_id].unsubscribe(namespace = product_id)

    def component_name(self, short_name, pool_resources, log):
        """Determine the full name of a subarray component. 
        This is most needed in the case of "dev" components - 
        for example, cbf_dev_2 instead of cbf_2.
        Returns the first match.

        Args:
            short_name (str): Short name of component (eg 'cbf').
            pool_resources (str): List of components (str) in current 
            subarray.
            log: Logger.

        Returns:
            full_name (str): Full name of component.
        """
        full_name = None
        for component in pool_resources:
            if short_name in component:
                full_name = component
        if full_name is None:
            log.warning('Could not find component: {}'.format(short_name))
        return full_name 

    def save_history(self, redis_server, product_id, key, value):
        """Save a particular key-value pair to a redis sensor history hash.
        
        Args:
            redis_server: current Redis server.
            product_id (str): the product ID given in the ?configure request.
            key (str): the name of the history item.
            value (str): the contents of the history item.

        Returns:
            None
        """
        hash_name = 'history:{}:{}'.format(product_id, key)
        # Avoid isoformat from datetime as behaviour not consisent
        # Avoid specifying strftime decimal places due to inconsistent behaviour
        # Recommmendation seems to be to simply truncate microseconds
        # if need be.
        time = datetime.utcnow()
        # Set ms field to 000 as specified
        time = time.strftime("%Y%m%dT%H%M%S.000Z")
        redis_server.hset(hash_name, time, value)

    def _configure(self, product_id):
        """Executes when configure request is processed

        Args:
            product_id (str): the product id given in the ?configure request

        Returns:
            None
        """
        # Update configuration:
        try:
            ant_sensors, cbf_conf_sensors, stream_sensors, cbf_sensors, conf_sensors, subarray_sensors, stream_conf_sensors = self.configure_katportal(os.path.join(os.getcwd(), self.config_file))
            if(ant_sensors is not None):
                self.ant_sensors = []
                self.ant_sensors.extend(ant_sensors)
            if(cbf_conf_sensors is not None):
                self.cbf_conf_sensors = []
                self.cbf_conf_sensors.extend(cbf_conf_sensors)
            if(stream_conf_sensors is not None):
                self.stream_conf_sensors = []
                self.stream_conf_sensors.extend(stream_conf_sensors)
            if(stream_sensors is not None):
                self.stream_sensors = []
                self.stream_sensors.extend(stream_sensors)
            if(conf_sensors is not None):
                self.conf_sensors = []
                self.conf_sensors.extend(conf_sensors)
            if(subarray_sensors is not None):
                self.subarray_sensors = []
                self.subarray_sensors.extend(subarray_sensors)
            if(cbf_sensors is not None):
                self.cbf_sensors = []
                self.cbf_sensors.extend(cbf_sensors)
            logger.info('Configuration updated')
        except:
            logger.warning('Configuration not updated; old configuration might be present.')
        cam_url = self.redis_server.get("{}:{}".format(product_id, 'cam:url'))
        client = KATPortalClient(cam_url, on_update_callback=partial(self.on_update_callback_fn, product_id), logger=logger)
        #client = KATPortalClient(cam_url, on_update_callback=lambda x: self.on_update_callback_fn(product_id), logger=logger)
        self.subarray_katportals[product_id] = client
        logger.info("Created katportalclient object for : {}".format(product_id))
        subarray_nr = product_id[-1]
        # Enter antenna list into the history hash
        ant_key = '{}:antennas'.format(product_id) 
        ant_list = self.redis_server.lrange(ant_key, 0, self.redis_server.llen(ant_key))
        self.save_history(self.redis_server, product_id, 'antennas', str(ant_list))
        # Get sensors on configure
        if(len(self.conf_sensors) > 0):
            conf_sensor_names = ['subarray_{}_'.format(subarray_nr) + sensor for sensor in self.conf_sensors]
            sensors_and_values = self.io_loop.run_sync(
                lambda: self._get_sensor_values(product_id, conf_sensor_names))
            for sensor_name, details in sensors_and_values.items():
                key = "{}:{}".format(product_id, sensor_name)
                write_pair_redis(self.redis_server, key, details['value'])
        # Get CBF component name (in case it has changed to CBF_DEV_[product_id] 
        # instead of CBF_[product_id])
        key = '{}:subarray_{}_{}'.format(product_id, subarray_nr, 'pool_resources')
        pool_resources = self.redis_server.get(key).split(',')
        self.cbf_name = self.component_name('cbf', pool_resources, logger)
        key = '{}:{}'.format(product_id, 'cbf_name')
        write_pair_redis(self.redis_server, key, self.cbf_name)
        # Get CBF sensor values required on configure.
        cbf_prefix = self.redis_server.get('{}:cbf_prefix'.format(product_id))
        if(len(self.cbf_conf_sensors) > 0):
            # Complete the CBF sensor names with the CBF component name.
            cbf_sensor_prefix = '{}_{}_'.format(self.cbf_name, cbf_prefix)
            cbf_conf_sensor_names = [cbf_sensor_prefix + sensor for sensor in self.cbf_conf_sensors]
            # Get CBF sensors and write to redis. 
            sensors_and_values = self.io_loop.run_sync(
                lambda: self._get_sensor_values(product_id, cbf_conf_sensor_names))
            for sensor_name, details in sensors_and_values.items():
                key = "{}:{}".format(product_id, sensor_name)
                write_pair_redis(self.redis_server, key, details['value'])
            # Calculate antenna-to-Fengine mapping
            antennas, feng_ids = self.antenna_mapping(product_id, cbf_sensor_prefix)
            write_pair_redis(self.redis_server, '{}:antenna_names'.format(product_id), antennas)
            write_pair_redis(self.redis_server, '{}:feng_ids'.format(product_id), feng_ids)
        # Get stream sensors on configure:
        if(len(self.stream_conf_sensors) > 0):
            stream_conf_sensors = ['subarray_{}_streams_{}_{}'.format(subarray_nr, cbf_prefix, sensor) 
                                  for sensor in self.stream_conf_sensors]
            sensors_and_values = self.io_loop.run_sync(
                lambda: self._get_sensor_values(product_id, stream_conf_sensors))
            for sensor_name, details in sensors_and_values.items():
                key = "{}:{}".format(product_id, sensor_name)
                write_pair_redis(self.redis_server, key, details['value'])
        # Indicate to anyone listening that the configure process is complete. 
        publish_to_redis(self.redis_server, REDIS_CHANNELS.alerts, 'conf_complete:{}'.format(product_id))

    def antenna_mapping(self, product_id, cbf_sensor_prefix):
        """Get the mapping from antenna to F-engine ID as given in 
        packet headers.

        Args:
            product_id (str): Identifier of current subarray.
            cbf_sensor_prefix (str): Prefix for the sensor name
            according to the current subarray configuration. 
            Eg: "cbf_1_wide_"

        Returns:
            antennas (list): List of antenna names.
            feng_ids (list): List of corresponding F-engine IDs.
        """
        labelling_sensor = '{}:{}input_labelling'.format(product_id, cbf_sensor_prefix)
        labelling = self.redis_server.get(labelling_sensor)
        labelling = ast.literal_eval(labelling)
        antennas = str([item[0] for item in labelling])
        feng_ids = str([int(np.floor(int(item[1])/2.0)) for item in labelling])
        return antennas, feng_ids

    def _capture_init(self, product_id):
        """Responds to capture-init request by getting schedule blocks

        Args:
            product_id (str): the product id given in the ?configure request

        Returns:
            None
        """
        try:
            schedule_blocks = self.io_loop.run_sync(lambda: self._get_future_targets(product_id), timeout = 5)
            key = "{}:schedule_blocks".format(product_id)
            write_pair_redis(self.redis_server, key, json.dumps(schedule_blocks))
            publish_to_redis(self.redis_server, REDIS_CHANNELS.sensor_alerts, key)
        except:
            logger.error("Could not retrieve schedule blocks")
  

    def _capture_start(self, product_id):
        """Responds to capture-start request

        Args:
            product_id (str): the product id given in the ?configure request

        Returns:
            None
        """
        sensors_for_update = self.build_sub_sensors(product_id)
        # Start io_loop to listen to sensors whose values should be registered
        # immediately when they change.
        if(len(sensors_for_update) > 0):
            loop = tornado.ioloop.IOLoop.current()
            loop.add_callback(lambda: self.subscription('subscribe', product_id, sensors_for_update))
            loop.start()

    def _capture_done(self, product_id):
        """Responds to capture-done request

        Args:
            product_id (str): the product id given in the ?configure request

        Returns:
            None, but does many things!
        """
        # Once-off sensors to query on ?capture_done
        sensors_to_query = []  # TODO: add sensors to query on ?capture_done
        sensors_and_values = self.io_loop.run_sync(
            lambda: self._get_sensor_values(product_id, sensors_to_query))
        for sensor_name, value in sensors_and_values.items():
            key = "{}:{}".format(product_id, sensor_name)
            write_pair_redis(self.redis_server, key, repr(value))

    def _deconfigure(self, product_id):
        """Responds to deconfigure request

        Args:
            product_id (str): the product id given in the ?configure request

        Returns:
            None
        """
        sensors_to_query = []  # TODO: add sensors to query on ?deconfigure
        sensors_and_values = self.io_loop.run_sync(
            lambda: self._get_sensor_values(product_id, sensors_to_query))
        for sensor_name, value in sensors_and_values.items():
            key = "{}:{}".format(product_id, sensor_name)
            write_pair_redis(self.redis_server, key, repr(value))
        if product_id not in self.subarray_katportals:
            logger.warning("Failed to deconfigure a non-existent product_id: {}".format(product_id))
        else:
            self.subarray_katportals.pop(product_id)
            logger.info("Deleted KATPortalClient instance for product_id: {}".format(product_id))

    def build_sub_sensors(self, product_id):
        """Builds the list of sensors for subscription.

        Args:
            product_id (str): the product id given in the ?configure request.

        Returns:
            sensors_for_update (list): list of full sensor names for subscription.
        """
        # Get cont update sensors
        sensors_for_update = []
        # Antenna sensors:
        sensors_for_update.extend(self.gen_ant_sensor_list(product_id, self.ant_sensors))
        # Stream sensors:
        cbf_prefix = self.redis_server.get('{}:cbf_prefix'.format(product_id))
        stream_sensors = self.gen_stream_sensor_list(product_id, self.stream_sensors, cbf_prefix)
        sensors_for_update.extend(stream_sensors)
        # Subarray sensors:
        for sensor in self.subarray_sensors:
            sensor = 'subarray_{}_{}'.format(product_id[-1], sensor)
            sensors_for_update.append(sensor)
        # CBF sensors:
        cbf_sensors = self.gen_cbf_sensor_list(self.cbf_sensors, self.cbf_name)
        sensors_for_update.extend(cbf_sensors)
        return sensors_for_update

    def _capture_stop(self, product_id):
        """Responds to capture-stop request

        Args:
            product_id (str): the product id given in the ?configure request

        Returns:
            None
        """
        pass
 
    def _other(self, product_id):
        """This is called when an unrecognized request is sent

        Args:
            product_id (str): the product id given in the ?configure request

        Returns:
            None
        """
        logger.warning("Unrecognized alert : {}".format(message['data']))

    def _conf_complete(self, product_id):
        """Called when sensor values for acquisition on configure have been acquired.
        
        Args: 
            product_id (str): the product ID given in the configure request.

        Returns:
            None
        """
        logger.info("Sensor values on configure acquired for {}.".format(product_id))

    @tornado.gen.coroutine
    def _get_future_targets(self, product_id):
        """Gets the schedule blocks of the product_id's subarray

        Args:
            product_id (str): the product id of a currently activated subarray

        Returns:
            List of dictionaries containing schedule block information

        Examples:
            >>> self.io_loop.run_sync(lambda: self._get_future_targets(product_id))
        """
        client = self.subarray_katportals[product_id]
        sb_ids = yield client.schedule_blocks_assigned()
        blocks = []
        for sb_id in sb_ids:
            # Should this be 'client' rather than 'portal_client'?
            # block = yield portal_client.future_targets(sb_id)
            block = yield client.future_targets(sb_id)
            blocks.append(block)
        raise tornado.gen.Return(blocks)

    @tornado.gen.coroutine
    def _get_sensor_values(self, product_id, targets):
        """Gets sensor values associated with the product_id's subarray

        Args:
            product_id (str): the product id of a currently activated subarray
            targets (list): expressions to look for in sensor names

        Returns:
            A dictionary of sensor-name / value pairs

        Examples:
            >>> self.io_loop.run_sync(lambda: self._get_sensor_values(product_id, ["target", "ra", "dec"]))
        """
        sensors_and_values = dict()
        if not targets:
            logger.warning("Sensor list empty. Not querying katportal...")
            raise tornado.gen.Return(sensors_and_values)
        client = self.subarray_katportals[product_id]
        sensor_names = yield client.sensor_names(targets)
        if not sensor_names:
            logger.warning("No matching sensors found!")
        else:
            for sensor_name in sensor_names:
                try:
                    sensor_value = yield client.sensor_value(sensor_name, include_value_ts=True)
                    sensors_and_values[sensor_name] = self._convert_SensorSampleValueTime_to_dict(sensor_value)
                except SensorNotFoundError as exc:
                    print("\n", exc)
                    continue
        raise tornado.gen.Return(sensors_and_values)

    def _convert_SensorSampleValueTime_to_dict(self, sensor_value):
        """Converts the named-tuple object returned by sensor_value
            query into a dictionary. This dictionary contains the following values:
                - timestamp:  float
                    The timestamp (UNIX epoch) the sample was received by CAM.
                    Timestamp value is reported with millisecond precision.
                - value_timestamp:  float
                    The timestamp (UNIX epoch) the sample was read at the lowest level sensor.
                    value_timestamp value is reported with millisecond precision.
                - value:  str
                    The value of the sensor when sampled.  The units depend on the
                    sensor, see :meth:`.sensor_detail`.
                - status:  str
                    The status of the sensor when the sample was taken. As defined
                    by the KATCP protocol. Examples: 'nominal', 'warn', 'failure', 'error',
                    'critical', 'unreachable', 'unknown', etc.

            Args:
                sensor_value (SensorSampleValueTime)

            Returns:
                sensor_value_dict (dict)
        """
        sensor_value_dict = dict()
        sensor_value_dict['timestamp'] = sensor_value.sample_time
        sensor_value_dict['value_timestamp'] = sensor_value.value_time
        sensor_value_dict['value'] = sensor_value.value
        sensor_value_dict['status'] = sensor_value.status
        return sensor_value_dict

    def _print_start_image(self):
        print(R"""
         ________________________________
        /                                "-_
       /      .  |  .                       \
      /      : \ | / :                       \
     /        '-___-'                         \
    /_________________________________________ \
         _______| |________________________--""-L
        /       F J                              \
       /       F   J                              L
      /      :'     ':                            F
     /        '-___-'                            /
    /_________________________________________--"
+---------------------------------------------------+
|                                                   |
|              Breakthrough Listen's                |
|                                                   |
|                KATPortal Client                   |
|                                                   |
|                 Version: {}                      |
|                                                   |
|  github.com/danielczech/meerkat-backend-interface |
|  github.com/ejmichaud/meerkat-backend-interface   |
|                                                   |
+---------------------------------------------------+
""".format(self.VERSION))
