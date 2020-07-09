"""
Demonstration of asynchronous sensor updates - this script 
is derived from sensor subscription code from E. Barr (2020)
"""

from katportalclient import KATPortalClient
from tornado import ioloop
from tornado.gen import coroutine
import uuid
import logging
import sys
import redis

SUBARRAY = "array_1"
FORMAT = "[%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s] %(message)s"
LOG_FILE = "test.log"                                                     

class SensorTracker(object):
    """This class heavily based on that from E. Barr (2020)
    """
    def __init__(self, host, component, sensor_name):
        log.debug(("Building sensor tracker activity tracker "
                   "on {} for sensor={} and component={}").format(
            host, sensor_name, component))
        self._client = KATPortalClient(
            host,
            on_update_callback=self.event_handler,
            logger=logging.getLogger(LOG_FILE))
        self._namespace = 'namespace_' + str(uuid.uuid4())
        self._sensor_name = sensor_name
        self._component = component
        self._full_sensor_name = None
        self._state = None
        self._has_started = False

    @coroutine
    def start(self):
        if self._has_started:
            return
        log.debug("Starting sensor tracker")
        yield self._client.connect()
        log.debug("Connected")
        result = yield self._client.subscribe(self._namespace)
        self._full_sensor_name = yield self._client.sensor_subarray_lookup(
            component=self._component, sensor=self._sensor_name,
            return_katcp_name=False)
        log.debug("Tracking sensor: {}".format(self._full_sensor_name))
        result = yield self._client.set_sampling_strategies(
            self._namespace, self._full_sensor_name,
            'event')
        sensor_sample = yield self._client.sensor_value(
            self._full_sensor_name,
            include_value_ts=False)
        self._state = sensor_sample.value
        log.debug("Initial state: {}".format(self._state))
        self._has_started = True

    @coroutine
    def stop(self):
        log.info("Unsubscribing and disconnecting")
        yield self._client.unsubscribe(self._namespace)
        yield self._client.disconnect()

    def event_handler(self, msg_dict):
        status = msg_dict['msg_data']['status']
        if status == "nominal":
            log.debug("Sensor value update: {} -> {}".format(
                self._state, msg_dict['msg_data']['value']))
            self._state = msg_dict['msg_data']['value']
            log.info("{}:{}".format(self._full_sensor_name, self._state))
            
class SubarrayActivity(SensorTracker):
    def __init__(self, host):
        super(SubarrayActivity, self).__init__(
            host, "subarray", "observation_activity")

if(__name__ == '__main__'):
    # Set up logging
    logging.basicConfig(filename = LOG_FILE, format = FORMAT)  
    log = logging.getLogger(LOG_FILE)
    log.setLevel('INFO')
    # Fetch CAM URL:
    redis_server = redis.StrictRedis(decode_responses = True)
    host = redis_server.get("{}:cam:url".format(SUBARRAY))
    # Set up tracker
    ObsActivity = SubarrayActivity(host)
    try:
        # ioloop
        loop = ioloop.IOLoop.current()
        loop.add_callback(lambda: ObsActivity.start())
        loop.start()
    except KeyboardInterrupt:
        # Graceful shutdown
        ObsActivity.stop()
        log.info("Shutdown")
        sys.exit(0)
