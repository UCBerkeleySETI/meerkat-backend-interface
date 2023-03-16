from __future__ import print_function

import tornado.gen
from katportalclient import KATPortalClient
from katportalclient.client import SensorNotFoundError
import redis
from functools import partial
import json
import yaml
import os
import sys
import ast
import numpy as np
from datetime import datetime
import uuid
import time

from .redis_tools import (
    REDIS_CHANNELS,
    write_pair_redis,
    write_list_redis,
    publish_to_redis
    )

#Slack channel to publish to: 
SLACK_CHANNEL = 'meerkat-obs-log'
# Redis channel to send messages to the Slack proxy
PROXY_CHANNEL = 'slack-messages'
    
from .logger import log 

class BLKATPortalClient(object):
    """Client server to KATPortal. Once initialized, the client creates 
    a Tornado ioloop and a connection to the local Redis server.

    Examples:
        >>> client = BLKATPortalClient()
        >>> client.start()

    When start() is called, a loop starts that subscribes to the 'alerts'
    channel of the Redis server. Depending on the message received, various
    tasks are performed. These include:
        1. Creating a new KATPortalClient object specific to the
            product ID just received in a ?configure request.
        2. Querying for schedule block information when ?capture-init is
            received and publishing this to Redis.
        3. Subscribing to various sensors for asynchronous (and immediate)
            updates upon receiving ?capture-start and publishing the 
            resultant information to Redis pub/sub channels.
        4. Deleting the corresponding KATPortalClient object when
            a ?deconfigure request is sent.

    TODO:
        1. Support thread-safe stopping of ioloop
    """

    VERSION = '2020-06-19'

    def __init__(self, config_file):
        """Our client server to the Katportal"""
        self.redis_server = redis.StrictRedis(decode_responses = True)
        self.p = self.redis_server.pubsub(ignore_subscribe_messages=True)
        self.io_loop = tornado.ioloop.IOLoop.current()
        self.subarray_katportals = dict()  # indexed by product IDs
        self.namespaces = dict() # indexed by product IDs
        self.config_file = config_file
        self.ant_sensors = []  # sensors required from each antenna
        self.stream_sensors = []  # stream sensors (for continuous update)
        self.cbf_conf_sensors = []  # cbf sensors to be queried once-off
        self.cbf_sensors = [] # cbf sensors (for continuous update)
        self.stream_conf_sensors = [] # stream sensors for acquisition
        self.conf_sensors = [] # other sensors to be queried once-off 
        self.subarray_sensors = [] # subarray-level sensors
        self.cont_update_sensors = [] # for all sensors for continuous update
        self.cbf_on_track = [] # cbf sensors for acquisition on each target
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
                log.info("Not processing this message --> {}".format(message))
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
                sensor_timestamp = msg['msg_data']['timestamp']
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
                        '{}:{}:{}'.format('data-suspect', product_id, 
                            sensor_value))
                #RA/Dec/Az/El
                elif('pos_request_base' in sensor_name):
                    publish_to_redis(self.redis_server, 
                        REDIS_CHANNELS.sensor_alerts, 
                        '{}:{}:{}'.format(product_id, 
                        sensor_name, sensor_value))
                    # Aternate method:
                    # self.antenna_consensus(product_id, 
                    #     'pos_request_base_dec')
                # Check for noise diode operation:
                elif('diode' in sensor_name):
                    publish_to_redis(self.redis_server, 
                        REDIS_CHANNELS.sensor_alerts,
                        '{}:{}:{}'.format(product_id, 
                        sensor_name, sensor_value))
                # Target information for publication
                elif('target' in sensor_name):
                    publish_to_redis(self.redis_server, 
                        REDIS_CHANNELS.sensor_alerts,
                        '{}:{}:{}'.format(product_id, 
                        sensor_name, sensor_value))
                    write_pair_redis(self.redis_server, '{}:target'.format(product_id), 
                        sensor_value)
                    write_pair_redis(self.redis_server, '{}:last-target'.format(product_id), 
                        str(time.time()))
                    self.save_history(self.redis_server, product_id, 'target',
                        sensor_value)
                # If a phaseup or delaycal has been performed, save the timestamp at which 
                # the respective script was run. This is needed so that we can obtain the 
                # phase solutions from Telstate.  
                elif('last_delay' in sensor_name):
                    write_pair_redis(self.redis_server, '{}:last_delay'.format(product_id), 
                        sensor_timestamp)
                elif('last_phaseup' in sensor_name): 
                    write_pair_redis(self.redis_server, '{}:last_phaseup'.format(product_id), 
                        sensor_timestamp)
                # Observation state for publication
                elif('activity' in sensor_name):
                    if(sensor_value == 'track'):
                        # Uncomment below to retrieve once-off CBF sensor values
                        # if(len(self.cbf_on_track) > 0):
                        #    # Complete the CBF sensor names with the CBF 
                        #    # component name.
                        #    cbf_on_track_names = ['{}_'.format(self.cbf_name) +
                        #        sensor for sensor in self.cbf_on_track]
                        #    # Get CBF sensors and write to redis.
                        #    self.fetch_once(cbf_on_track_names, product_id,
                        #        3, 30, 0.5)
                        publish_to_redis(self.redis_server, 
                        REDIS_CHANNELS.alerts, 
                        '{}:{}'.format('tracking', product_id))
                    else:
                        publish_to_redis(self.redis_server, 
                        REDIS_CHANNELS.alerts, 
                        '{}:{}'.format('not-tracking', product_id))
                # If script not running, attempt to unsubscribe from sensors
                elif('script_status' in sensor_name):
                    if(sensor_value != 'busy'):
                       self.unsubscribe_list(product_id)

    def _configure(self, product_id):
        """Executes when configure request is processed

        Args:
            product_id (str): the product id given in the ?configure request

        Returns:
            None
        """
        # Update configuration:
        try:
            (ant_sensors, 
            cbf_conf_sensors, 
            stream_sensors, 
            cbf_sensors, 
            conf_sensors, 
            subarray_sensors, 
            stream_conf_sensors,
            cbf_on_track) = self.configure_katportal(
                os.path.join(os.getcwd(), self.config_file))
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
            if(cbf_on_track is not None):
                self.cbf_on_track = []
                self.cbf_on_track.extend(cbf_on_track)
            log.info('Configuration updated')
        except:
            log.warning('Configuration not updated; old configuration might be present.')
        cam_url = self.redis_server.get("{}:{}".format(product_id, 'cam:url'))
        client = KATPortalClient(cam_url, on_update_callback=partial(
            self.on_update_callback_fn, product_id), logger=log)
        #client = KATPortalClient(cam_url, 
        #    on_update_callback=lambda x: self.on_update_callback_fn(product_id), 
        #    logger=log)
        self.subarray_katportals[product_id] = client
        log.info("Created katportalclient object for : {}".format(product_id))
        subarray_nr = product_id[-1]
        ant_key = '{}:antennas'.format(product_id) 
        ant_list = self.redis_server.lrange(ant_key, 0, 
            self.redis_server.llen(ant_key))
        # Enter antenna list into the history hash
        ant_history = json.dumps(ant_list)
        self.save_history(self.redis_server, product_id, 'antennas', 
            ant_history)
        # Get sensors on configure
        if(len(self.conf_sensors) > 0):
            conf_sensor_names = ['subarray_{}_'.format(subarray_nr) 
                + sensor for sensor in self.conf_sensors]
            self.fetch_once(conf_sensor_names, product_id, 3, 210, 0.5)
        # Get CBF component name (in case it has changed to 
        # CBF_DEV_[product_id] instead of CBF_[product_id])
        key = '{}:subarray_{}_{}'.format(product_id, subarray_nr, 
            'pool_resources')
        pool_resources = self.redis_server.get(key).split(',')
        self.cbf_name = self.component_name('cbf', pool_resources, log)
        key = '{}:{}'.format(product_id, 'cbf_name')
        write_pair_redis(self.redis_server, key, self.cbf_name)
        # Get CBF sensor values required on configure.
        cbf_prefix = self.redis_server.get('{}:cbf_prefix'.format(product_id))
        if(len(self.cbf_conf_sensors) > 0):
            # Complete the CBF sensor names with the CBF component name.
            cbf_sensor_prefix = '{}_{}_'.format(self.cbf_name, cbf_prefix)
            cbf_conf_sensor_names = [cbf_sensor_prefix + 
                sensor for sensor in self.cbf_conf_sensors]
            # Get CBF sensors and write to redis.
            self.fetch_once(cbf_conf_sensor_names, product_id, 3, 210, 0.5)
            # Calculate antenna-to-Fengine mapping
            antennas, feng_ids = self.antenna_mapping(product_id, 
                cbf_sensor_prefix)
            write_pair_redis(self.redis_server, 
                '{}:antenna_names'.format(product_id), antennas)
            write_pair_redis(self.redis_server, 
                '{}:feng_ids'.format(product_id), feng_ids)
        # Get stream sensors on configure:
        if(len(self.stream_conf_sensors) > 0):
            stream_conf_sensors = ['subarray_{}_streams_{}_{}'.format(
                subarray_nr, cbf_prefix, sensor) for sensor in 
                self.stream_conf_sensors]
            self.fetch_once(stream_conf_sensors, product_id, 3, 210, 0.5)  
        # Retrieve Telstate Redis DB endpoint information for the current 
        # subarray. Each time a new subarray is built, a new Telstate Redis 
        # DB is created.
        # TODO: Consider moving this specific Telstate sensor into the 
        # config file, or formalise in another manner. 
        # Sensor name lookup does not appear to handle this sensor name, therefore
        # build the sensor name manually: 
        # First, find the SDP-provided product_id:
        sdp_id_sensor = 'sdp_{}_subarray_product_ids'.format(subarray_nr)
        # TODO: Rewrite sensor fetching functions to handle different subarray components, 
        # replacing the sensor retrieval code here. 
        sdp_id_details = self.io_loop.run_sync(lambda: self.fetch_sensor_pattern(sdp_id_sensor, client, log))
        for sensor, details in sdp_id_details.items():
            sdp_ids = details.value
        # Take only the first 'wide' version (not using 'narrow' mode zoom sections):
        sdp_ids = sdp_ids.split(',')
        log.info("SDP IDs (all): {}".format(sdp_ids))
        sdp_wide_ids = []
        for sdp_id in sdp_ids:
            if('wide' in sdp_id):
                sdp_wide_ids.append(sdp_id)
        log.info("SDP 'wide' IDs: {}".format(sdp_wide_ids))
        if(len(sdp_wide_ids) > 0):
            sdp_id = sdp_wide_ids[0]
        else:
            sdp_id = sdp_ids[0]    
        log.info("Using {} as SDP ID".format(sdp_id))
        # Second, build telstate sensor name:
        telstate_sensor = 'sdp_{}_spmc_{}_telstate_telstate'.format(subarray_nr, sdp_id)
        # Save telstate sensor name to Redis
        write_pair_redis(self.redis_server, '{}:telstate_sensor'.format(product_id), telstate_sensor) 
        telstate_sensor_details = self.io_loop.run_sync(lambda: self.fetch_sensor_pattern(telstate_sensor, client, log))
        for sensor, details in telstate_sensor_details.items():
            telstate_endpoint = details.value
        self.redis_server.set(telstate_sensor, telstate_endpoint)
        # Initialise last-target to 0
        write_pair_redis(self.redis_server, '{}:last-target'.format(product_id), 0) 
        log.info("All requests for once-off sensor values sent")
        # Indicate to anyone listening that the configure process is complete.
        publish_to_redis(self.redis_server, REDIS_CHANNELS.alerts, 
            'conf_complete:{}'.format(product_id))

    @tornado.gen.coroutine
    def fetch_sensor_pattern(self, pattern, client, log):
        """Fetch sensor pattern for each antenna. 
           
           Args:
               pattern (str): per-antenna sensor name with antenna fields
                              replaced with braces.
               client (obj): KATPortalClient object.
               log: logger

           Returns:
               sensor_details (dict): sensor results including value and timestamp
                                      of last change in value.
               None if no sensor results obtainable. 
        """
        # TODO: Merge this function with _get_sensor_values and enable the retrieval of 
        # sensors from different subarray components. 
        try:
            sensor_details = yield client.sensor_values(pattern, include_value_ts=True)
            return(sensor_details)
        except Exception as e:
            log.error(e)
            return(None)

    def _conf_complete(self, product_id):
        """Called when sensor values for acquisition on configure have been 
        acquired.
        
        Args: 
            product_id (str): the name of the current subarray provided in
            the ?configure request.

        Returns:
            None
        """
        log.info("Sensor values on configure acquired for {}.".format(product_id))
        # Alert via slack:
        slack_message = "{}:*Successful subarray configuration for {}*".format(SLACK_CHANNEL, product_id)
        publish_to_redis(self.redis_server, PROXY_CHANNEL, slack_message)

    def _capture_init(self, product_id):
        """Responds to capture-init request by acquiring schedule block
        information including the list of pointings and the current 
        schedule block IDs.

        Args:
            product_id (str): the name of the current subarray provided in
            the ?configure request.

        Returns:
            None
        """
        # Schedule block IDs (sched_observation_schedule_1) 
        # This is the list of schedule block IDs. The currently running block
        # will be in position 1.
        self.fetch_once('sched_observation_schedule_1', product_id, 3, 210, 0.5)
        # Schedule blocks - pointing list
        retries = 3
        # Increase the timeout by this factor on subsequent retries
        timeout_factor = 0.5 
        for i in range(0, retries):
            try:
                schedule_blocks = self.io_loop.run_sync(
                    lambda: self._get_future_targets(product_id),
                    timeout = 30 + timeout_factor*i*30)
                key = "{}:schedule_blocks".format(product_id)
                write_pair_redis(self.redis_server, key, 
                    json.dumps(schedule_blocks))
                publish_to_redis(self.redis_server, 
                    REDIS_CHANNELS.sensor_alerts, key)
                # If success, break.
                break
            except Exception as e:
                log.warning("Could not retrieve schedule blocks: attempt {} of {}".format(i + 1, retries))
                log.error(e)
        # If retried <retries> times, then log an error.
        if(i == (retries - 1)):
            log.error("Could not retrieve schedule blocks: {} attempts, giving up.".format(retries))

    def _capture_start(self, product_id):
        """Responds to capture-start request. Subscriptions to required 
        sensors are made. Note that this must be done here (on capture-start),
        because some sensors (notably observation-activity) only become available 
        for subscription on capture-start,

        Args:
            product_id (str): the name of the current subarray provided in the
            ?configure request.

        Returns:
            None
        """
        # Save capture-start time:
        write_pair_redis(self.redis_server, '{}:last-capture-start'.format(product_id), 
            str(time.time())) 
        # Once-off sensors to query on ?capture_done
        # Uncomment below to add sensors for query.
        # sensors_to_query = [] 
        # self.fetch_once(sensors_to_query, product_id, 3, 5, 0.5)
        sensors_for_update = self.build_sub_sensors(product_id)
        # Test the speed of retrieval for target information from an 
        # individual antenna:
        # Retrieve list of antennas:
        ant_key = '{}:antennas'.format(product_id) 
        antennas = self.redis_server.lrange(ant_key, 0, 
            self.redis_server.llen(ant_key))
        # Build antenna sensor name
        # Pick first antenna in list for now 
        # (implement antenna consensus again if this approach proves faster)
        ant_target = "{}_target".format(antennas[0])        
        sensors_for_update.append(ant_target)
        # Start io_loop to listen to sensors whose values should be registered
        # immediately when they change.
        if(len(sensors_for_update) > 0):
            loop = tornado.ioloop.IOLoop.current()
            loop.add_callback(lambda: self.subscribe_list(product_id, 
                sensors_for_update))
            loop.start()

    def _capture_stop(self, product_id):
        """Responds to capture-stop request.

        Args:
            product_id (str): the name of the current subarray provided in
            the ?configure request.

        Returns:
            None
        """
        # Once-off sensors to query on ?capture-stop
        # Uncomment these lines to add sensors for query
        #sensors_to_query = []  
        #self.fetch_once(sensors_to_query, product_id, 3, 5, 0.5)
        pass

    def _capture_done(self, product_id):
        """Responds to capture-done request. Resets the schedule block list to
        'Unknown_SB'.

        Args:
            product_id (str): the name of the current subarray provided in
            the ?configure request.

        Returns:
            None
        """
        # Reset schedule block to empty list
        key = '{}:sched_observation_schedule_1'.format(product_id)
        write_pair_redis(self.redis_server, key, 'Unknown_SB')

    def _deconfigure(self, product_id):
        """Responds to deconfigure request

        Args:
            product_id (str): the name of the current subarray provided in 
            the ?configure request.

        Returns:
            None
        """
        # Once-off sensors to query on ?deconfigure
        # Uncomment the following lines to add sensors for query
        #sensors_to_query = [] 
        #self.fetch_once(sensors_to_query, product_id, 3, 5, 0.5)  
        if product_id not in self.subarray_katportals:
            log.warning("Failed to deconfigure a non-existent product_id: {}".format(product_id))
        else:
            # Delete certain Redis keys to avoid leaving stale values for the next subarray
            # configuration:
            keys_to_rm = ['{}:last_phaseup'.format(product_id), 
                '{}:last_delaycal'.format(product_id)]
            self.redis_server.delete(*keys_to_rm)
            # Delete current subarray client:
            self.subarray_katportals.pop(product_id)
            log.info("Deleted KATPortalClient instance for product_id: {}".format(product_id))
        # Alert via slack:
        slack_message = "{}:{} deconfigured".format(SLACK_CHANNEL, product_id)
        publish_to_redis(self.redis_server, PROXY_CHANNEL, slack_message)
 
    def _other(self, product_id):
        """This is called when an unrecognized request is sent.

        Args:
            product_id (str): the name of the current subarray provided in
            the ?configure request.

        Returns:
            None
        """
        log.warning("Unrecognized alerts message")

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
                    return(cfg['per_antenna_sub'], 
                        cfg['cbf_on_configure'],           
                        cfg['stream_sub'], 
                        cfg['cbf_sub'], 
                        cfg['array_on_configure'],
                        cfg['array_sub'], 
                        cfg['stream_on_configure'],
                        cfg['cbf_on_track'])
                except yaml.YAMLError as E:
                    log.error(E)
        except IOError:
            log.error('Config file not found')

    @tornado.gen.coroutine
    def unsubscribe_list(self, product_id):
        """Unsubscribe from all sensor websocket subscriptions for 
        the subarray designated by product_id.

        Args: 
            product_id (str): Name of the current subarray.

        Returns:
            None
        """
        yield self.subarray_katportals[product_id].unsubscribe(
            namespace = self.namespaces[product_id])
        yield self.subarray_katportals[product_id].disconnect()
        # Stop io_loop
        self.io_loop.stop()
        log.info('Unsubscribed from sensors.')   

    @tornado.gen.coroutine
    def subscribe_list(self, product_id, sensor_list):
        """Subscribes to each sensor listed for asynchronous updates.
        
        Args:
            sensor_list (list): Full names of sensors to subscribe to.
            product_id (str): The product ID given in the ?configure request.

        Returns:
            None
        """
        self.namespaces[product_id] = '{}_{}'.format(product_id, 
            str(uuid.uuid4()))
        yield self.subarray_katportals[product_id].connect()
        result = yield self.subarray_katportals[product_id].subscribe(
            namespace = self.namespaces[product_id]) 
        for sensor in sensor_list:
            # Using product_id to retrieve unique namespace
            result = yield self.subarray_katportals[product_id].set_sampling_strategies(
                self.namespaces[product_id], sensor, 'event')
        log.info('Subscribed to {} sensors'.format(len(sensor_list)))

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
        ant_list = self.redis_server.lrange(ant_key, 0, 
            self.redis_server.llen(ant_key))          
        ant_status = []
        try:
            for i in range(len(ant_list)):
                data_suspect = ast.literal_eval(self.redis_server.get(
                    '{}:{}_data_suspect'.format(product_id, ant_list[i])))
                marked_faulty_key = '{}:{}_marked_faulty'.format(product_id, 
                    ant_list[i])
                marked_faulty = ast.literal_eval(self.redis_server.get(
                    marked_faulty_key))
                if(data_suspect & marked_faulty):
                    # If an antenna is marked faulty while subarray built, 
                    # allow it and log. 
                    publish_to_redis(self.redis_server, 
                        REDIS_CHANNELS.sensor_alerts, marked_faulty_key)
                    ant_status.append(False) 
                    # Note that if marked_faulty is True, 
                    # ant_suspect is by definition True. 
                else:
                    ant_status.append(data_suspect)
            if(sum(ant_status) == 0): # all antennas show good data
                publish_to_redis(self.redis_server, 
                    REDIS_CHANNELS.sensor_alerts, 
                    '{}:data_suspect:{}'.format(product_id, False))
            else: 
                publish_to_redis(self.redis_server, 
                    REDIS_CHANNELS.sensor_alerts, 
                    '{}:data_suspect:{}'.format(product_id, True))
        except:
            # If any of the sensors are not available, set subarray data 
            # suspect flag to True
            publish_to_redis(self.redis_server, REDIS_CHANNELS.sensor_alerts,
                '{}:data_suspect:{}'.format(product_id, True))

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
        sensor_list = ['{}:{}_{}'.format(product_id, component, sensor) 
            for component in components]
        components_status = self.redis_server.mget(sensor_list)
        for i, status in enumerate(components_status):
            if(status == value):
                mask[i] = 0
        if(np.sum(mask) <= n_stragglers):
            consensus = True
        else:
            log.info('Consensus for {} not reached'.format(sensor))
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
        ant_list = self.redis_server.lrange(ant_key, 0, 
            self.redis_server.llen(ant_key))
        ant_status = ''
        ant_compare = ''
        try:
            for i in range(len(ant_list)):
                ant_status = ant_status + self.redis_server.get(
                    '{}:{}_{}'.format(product_id, ant_list[i], sensor_name))  
            ant_compare = ant_compare + self.redis_server.get(
                '{}:{}_{}'.format(product_id, ant_list[0], 
                sensor_name))*len(ant_list)
            if(ant_status == ant_compare): # all antennas show the same value
                # Get value from last antenna
                value = ast.literal_eval(self.redis_server.get(
                    '{}:{}_{}'.format(product_id, ant_list[i], sensor_name)))
                if(value is not None):
                    publish_to_redis(self.redis_server, 
                    REDIS_CHANNELS.sensor_alerts, 
                    '{}:{}:{}'.format(product_id, sensor_name, value))
                    key = '{}:{}'.format(product_id, sensor_name)
                    write_pair_redis(self.redis_server, key, value)
                else:
                    publish_to_redis(self.redis_server, 
                    REDIS_CHANNELS.sensor_alerts, 
                    '{}:{}:unavailable'.format(product_id, sensor_name))
            else:
                log.warning("Antennas do not show consensus for sensor: {}".format(sensor_name))
        except:
            # If any of the sensors are not available:
            publish_to_redis(self.redis_server, REDIS_CHANNELS.sensor_alerts, 
                '{}:{}:unavailable'.format(product_id, sensor_name))

    def gen_ant_sensor_list(self, product_id, ant_sensors):
        """Automatically builds a list of sensor names for each antenna.

        Args:
            product_id (str): the product id given in the ?configure request
            ant_sensors (list): the sensors to be queried for each antenna

        Returns:
            ant_sensor_list (list): the full sensor names associated with 
            each antenna
        """
        ant_sensor_list = []
        # Add sensors specific to antenna components for each antenna:
        ant_key = '{}:antennas'.format(product_id)
        ant_list = self.redis_server.lrange(ant_key, 0, 
            self.redis_server.llen(ant_key))  # list of antennas
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
            sensor_name = 'subarray_{}_streams_{}_{}'.format(product_id[-1], 
                cbf_prefix, sensor)
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

    def build_sub_sensors(self, product_id):
        """Builds the list of sensors for subscription.

        Args:
            product_id (str): the name of the current subarray provided in
            the ?configure request.

        Returns:
            sensors_for_update (list): list of full sensor names for 
            subscription.
        """
        # Get cont update sensors
        sensors_for_update = []
        # Antenna sensors:
        sensors_for_update.extend(self.gen_ant_sensor_list(product_id, 
            self.ant_sensors))
        # Stream sensors:
        cbf_prefix = self.redis_server.get('{}:cbf_prefix'.format(product_id))
        stream_sensors = self.gen_stream_sensor_list(product_id, 
            self.stream_sensors, cbf_prefix)
        sensors_for_update.extend(stream_sensors)
        # Subarray sensors:
        for sensor in self.subarray_sensors:
            sensor = 'subarray_{}_{}'.format(product_id[-1], sensor)
            sensors_for_update.append(sensor)
        # CBF sensors:
        cbf_sensors = self.gen_cbf_sensor_list(self.cbf_sensors, self.cbf_name)
        sensors_for_update.extend(cbf_sensors)
        return sensors_for_update

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
        # Avoid specifying strftime decimal places due to inconsistent 
        # behaviour
        # Recommmendation seems to be to simply truncate microseconds
        # if need be.
        time = datetime.utcnow()
        # Set ms field to 000 as specified
        time = time.strftime("%Y%m%dT%H%M%S.000Z")
        redis_server.hset(hash_name, time, value)

    def fetch_once(self, sensor_names, product_id, retries, sync_timeout, timeout_factor):
        """Handles once-off sensor requests, permitting retries in case there are problems 
        on the CAM side. Once the sensor values are retrieved, the name-value pair are 
        written to the Redis database.

        Args:
            timeout_factor (float): Fraction by which to increase the timeout with on 
            each re-attempt.
            sensor_names (list): List of full sensor names whose values are required.
            product_id (str): The product ID for the current subarray.
            retries (int): The number of times to attempt fetching the sensor values.
            sync_timeout (int): The maximum time to wait for sensor values from CAM. 

            None.
        """ 
        for i in range(retries):
            try:
                self.io_loop.run_sync(lambda: self._get_sensor_values(
                    product_id, sensor_names), 
                    timeout = sync_timeout + int(sync_timeout*timeout_factor*i))
                # If sensors succesfully queried and written to Redis, break.
                break 
            except Exception as e:
                log.warning("Could not retrieve once-off sensors: attempt {} of {}".format(
                    i + 1, retries))
                log.error(e)
        # If retried <retries> times, then log an error.
        if(i == (retries - 1)):
            log.error("Could not retrieve once-off sensors: {} attempts, giving up.".format(
                retries)) 
            log.error("{} could not be retrieved.".format(sensor_names))

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
        labelling_sensor = '{}:{}input_labelling'.format(product_id, 
            cbf_sensor_prefix)
        labelling = self.redis_server.get(labelling_sensor)
        labelling = ast.literal_eval(labelling)
        antennas = str([item[0] for item in labelling])
        feng_ids = str([int(np.floor(int(item[1])/2.0)) for item in 
            labelling])
        return antennas, feng_ids

    @tornado.gen.coroutine
    def _get_future_targets(self, product_id):
        """Gets the schedule blocks of the product_id's subarray.

        Args:
            product_id (str): the name of the current subarray provided in 
            the ?configure request.

        Returns:
            List of dictionaries containing schedule block information.

        Examples:
            >>> self.io_loop.run_sync(lambda: 
            self._get_future_targets(product_id))
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
        """Gets sensor values associated with the current subarray and
        writes them to the Redis database.

        Args:
            product_id (str): the name of the current subarray provided in 
            the ?configure request.
            targets (list): expressions to look for in sensor names.

        Returns:
            None

        Examples:
            >>> self.io_loop.run_sync(lambda: 
            self._get_sensor_values(product_id, ["target", "ra", "dec"]))
        """
        if not targets:
            log.warning("Sensor list empty. Not querying katportal...")
            raise tornado.gen.Return(sensors_and_values)
        else:
            client = self.subarray_katportals[product_id]
            # Query approach:
            # Instead of sequentially querying each sensor, build a regex query
            # and fetch them all at once at the suggestion of ebarr. 
            # This is said to cause fewer timeout problems. 
            query = "|".join(targets)
            try:
                sensor_details = yield client.sensor_values(query, 
                    include_value_ts=True)
                for sensor, details in sensor_details.items():
                    sensor_dict = self._convert_SensorSampleValueTime_to_dict(details)
                    redis_key = "{}:{}".format(product_id, sensor)
                    # Only writing the sensor value (no other metadata for now)
                    write_pair_redis(self.redis_server, redis_key, 
                        sensor_dict['value'])
            except Exception as e:
                log.error(e)

    def _convert_SensorSampleValueTime_to_dict(self, sensor_value):
        """Converts the named-tuple object returned by sensor_value
           query into a dictionary. This dictionary contains the 
           following values:
                - timestamp:  float
                    The timestamp (UNIX epoch) the sample was received by CAM.
                    Timestamp value is reported with millisecond precision.
                - value_timestamp:  float
                    The timestamp (UNIX epoch) the sample was read at the 
                    lowest level sensor. value_timestamp value is reported 
                    with millisecond precision.
                - value:  str
                    The value of the sensor when sampled. The units depend 
                    on the sensor, see :meth:`.sensor_detail`.
                - status:  str
                    The status of the sensor when the sample was taken. As 
                    defined by the KATCP protocol. Examples: 'nominal', 'warn', 
                    'failure', 'error', 'critical', 'unreachable', 'unknown', 
                    etc.

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
|              Version: {}                  |
|                                                   |
|  github.com/danielczech/meerkat-backend-interface |
|  github.com/ejmichaud/meerkat-backend-interface   |
|                                                   |
+---------------------------------------------------+
""".format(self.VERSION))
