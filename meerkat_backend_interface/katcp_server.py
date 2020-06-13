r"""
Adapted from the reynard package (MIT license) on June 22, 2018:
https://github.com/ewanbarr/reynard

  ___              _   _   _                      _      _    _    _
 | _ )_ _ ___ __ _| |_| |_| |_  _ _ ___ _  _ __ _| |_   | |  (_)__| |_ ___ _ _
 | _ \ '_/ -_) _` | / /  _| ' \| '_/ _ \ || / _` | ' \  | |__| (_-<  _/ -_) ' \
 |___/_| \___\__,_|_\_\\__|_||_|_| \___/\_,_\__, |_||_| |____|_/__/\__\___|_||_|
                                            |___/

            ______________                      ,-.
           /             /|                    / \  `.  __..-,O
          /             / |                   :   \ --''_..-'.'
         /____________ /  |                   |    . .-' `. '.
        | ___________ |   |                   :     .     .`.'
        ||           ||   |                    \     `.  /  ..
        ||           ||   |                     \      `.   ' .
        ||           ||   |                      `,       `.   \
        ||___________||   |                     ,|,`.        `-.\
        |   _______   |  /                     '.||  ``-...__..-`
       /|  (_______)  | /____________           |  |
      ( |_____________|/             \          |__|
       \                              \         /||\
   .=======================.           \       //||\\
   | ::::::::::::::::  ::: |            \     // || \\
   | ::::::::::::::[]  ::: |             \___//__||__\\___
   |   -----------     ::: |              '--------------'
   \-----------------------
"""

from __future__ import print_function

import sys
import json
import time
from katcp import Sensor, AsyncDeviceServer, AsyncReply
from katcp.kattypes import request, return_reply, Int, Str

import redis
from .redis_tools import REDIS_CHANNELS, write_pair_redis, write_list_redis, publish_to_redis

# to handle halt request
from concurrent.futures import Future
from tornado import gen
from tornado.concurrent import chain_future

from .logger import log
from .reynard_utils_p3 import unpack_dict


class BLBackendInterface(AsyncDeviceServer):
    """Breakthrough Listen's KATCP Server Backend Interface

    This server responds to requests sent from CAM, most notably:
        @ configue
        @ capture-init
        @ capture-start
        @ capture-stop
        @ capture-done
        @ deconfigure

    But because it inherits from AsyncDeviceServer, also responds to:
        * halt
        * help
        * log-level
        * restart [#restartf1]_
        * client-list
        * sensor-list
        * sensor-sampling
        * sensor-value
        * watchdog
        * version-list (only standard in KATCP v5 or later)
        * request-timeout-hint (pre-standard only if protocol flags indicates
                              timeout hints, supported for KATCP v5.1 or later)
        * sensor-sampling-clear (non-standard)
    """

    VERSION_INFO = ("BLUSE-katcp-interface", 1, 0)
    BUILD_INFO = ("BLUSE-katcp-implementation", 1, 0, "rc?")
    DEVICE_STATUSES = ["ok", "fail", "degraded"]

    def __init__(self, server_host, server_port):
        self.port = server_port
        self.redis_server = redis.StrictRedis()
        super(BLBackendInterface, self).__init__(
            server_host, server_port)

    def start(self):
        """Start the server

        Based on the passed configuration object this is
        where the clients for subordinate nodes will be
        set up.
        """
        super(BLBackendInterface, self).start()
        if(sys.stdout.isatty()):
            print(R"""
                      ,'''''-._
                     ;  ,.  <> `-._
                     ;  \'   _,--'"
                    ;      (
                    ; ,   ` \
                    ;, ,     \
                   ;    |    |        MeerKAT BL Backend Interface:
                   ; |, |    |\       KATCP Server
                  ;  |  |    | \      Version: {}
                  |.-\ ,\    |\ :     Port: {}
                  |.| `. `-. | ||
                  :.|   `-. \ ';;
                   .- ,   \;;|
                   ;   ,  |  ,\       https://github.com/danielczech/meerkat-backend-interface
                   ; ,    ;    \      https://github.com/ejmichaud/meerkat-backend-interface
                  ;    , /`.  , )
               __,;,   ,'   \  ,|
         _,--''__,|   /      \  :
       ,'_,-''    | ,/        | :
      / /         | ;         ; |
     | |      __,-| |--..__,--| |---.--....___
___,-| |----''    / |         `._`-.          `----
      \ \        `'''             '''      --
       `.`.                 --'
         `.`-._        _,             ,-     __,-
            `-.`.
   --'         `;
        """.format("{}.{}".format(self.VERSION_INFO[1], self.VERSION_INFO[2]), self.port))

    @request(Str(), Str(), Int(), Str(), Str())
    @return_reply()
    def request_configure(self, req, product_id, antennas_csv,
                          n_channels, streams_json, proxy_name):
        """Receive metadata for upcoming observation.

        This command is used to configure a BLUSE instance when a 
        new subarray is activated.

        Args:
            product_id (str): This is the name of the current subarray,
                    which is used when requesting sensor data specific to 
                    components which belong to the current subarray.
                    Eg "array_1".
                    This name is unique across all current subarrays (no 
                    two concurrently active subarrays will have the same name). 
                    However, it is not a globally unique identifier for all 
                    time. An identical name may be provided for later 
                    activations of other subarrays. 

            antennas_csv (str): A comma separated list of physical antenna 
                    names used in the current subarray.

            n_channels (int): The integer number of frequency channels provided
                    by the CBF.

            streams_json (str) is a JSON struct containing config keys and
                    values describing the streams.  For example:
                    {'stream_type1': {
                    'stream_name1': 'stream_address1',
                    'stream_name2': 'stream_address2',
                    ...},
                 'stream_type2': {
                    'stream_name1': 'stream_address1',
                    'stream_name2': 'stream_address2',
                    ...},
                    ...}
                The steam type keys indicate the source of the data and the 
                type, e.g. a cam.http. stream_address will be a URL.  
                For SPEAD streams, the format will be spead://<ip>[+<count>]:<port>, 
                representing SPEAD stream multicast groups.
                The count parameter indicates the number of additional consecutively 
                numbered multicast group IP addresses (sharing the same UDP port 
                number).
                stream_name is the name used to identify the stream in CAM.
                A Python example is shown below, for five streams:
                One CAM stream, with type cam.http. 
                The CAM stream provides the connection string for katportalclient 
                (for the specific subarray that this BLUSE instance is being 
                configured for).
                One F-engine stream, with type: cbf.antenna_channelised_voltage.
                One X-engine stream, with type: cbf.baseline_correlation_products.
                Two beam streams, with type: cbf.tied_array_channelised_voltage.
                The stream names ending in x are horizontally polarised, and those
                ending in y are vertically polarised.

            proxy_name (str): The CAM name for the instance of the BLUSE data
                proxy that is being configured. For example, "bluse_3".
                There will only be a single BLUSE instance of the proxy per subarray.

        Returns:
            None (responds with "ok" or "fail" and logs either info or an error).

        Writes:
            - [product_id]:timestamp" -> "1534657577373.23423" (Redis string)
            - [product_id]:antennas" -> [1,2,3,4] (Redis list)
            - [product_id]:n_channels" -> "4096" (Redis string)
            - [product_id]:proxy_name "-> "bluse_N" (Redis string)
            - [product_id]:streams" -> {....} (Redis string)
            - current:obs:id -> [product_id]
            - [product_id]:cbf_prefix -> (Redis string) 

        Publishes:
            redis-channel: 'alerts' <-- "configure"
            
        Examples:
            > ?configure array_1_bc856M4k a1,a2,a3,a4 128000 {"cam.http":{"camdata":"http://monctl.devnmk.camlab.kat.ac.za/api/client/2"},"stream_type2":{"stream_name1":"stream_address1","stream_name2":"stream_address2"}} BLUSE_3
        """
        try:
            antennas_list = antennas_csv.split(",")
            json_dict = unpack_dict(streams_json)
            cam_url = json_dict['cam.http']['camdata']
        except Exception as e:
            log.error(e)
            return ("fail", e)
        # Extracting CBF prefix from configure message as recommended by CAM
        try:
            stream_type = 'cbf.antenna_channelised_voltage'
            cbf_prefix = next(iter(json_dict[stream_type])).split('.')[0]
            log.info('CBF prefix extracted: {}'.format(cbf_prefix))            
        except Exception as e:
            cbf_prefix = 'wide'
            log.error('Could not extract CBF prefix; defaulting to \'wide\'')
        statuses = []
        statuses.append(write_pair_redis(self.redis_server, "{}:timestamp".format(product_id), time.time()))
        statuses.append(write_list_redis(self.redis_server, "{}:antennas".format(product_id), antennas_list))
        statuses.append(write_pair_redis(self.redis_server, "{}:n_channels".format(product_id), n_channels))
        statuses.append(write_pair_redis(self.redis_server, "{}:proxy_name".format(product_id), proxy_name))
        statuses.append(write_pair_redis(self.redis_server, "{}:streams".format(product_id), json.dumps(json_dict)))
        statuses.append(write_pair_redis(self.redis_server, "{}:cam:url".format(product_id), cam_url))
        statuses.append(write_pair_redis(self.redis_server, "current:obs:id", product_id))
        statuses.append(write_pair_redis(self.redis_server, "{}:cbf_prefix".format(product_id), cbf_prefix))
        msg = "configure:{}".format(product_id)
        statuses.append(publish_to_redis(self.redis_server, REDIS_CHANNELS.alerts, msg))
        if all(statuses):
            return ("ok",)
        else:
            return ("fail", "Failed to publish to our local redis server")

    @request(Str())
    @return_reply()
    def request_capture_init(self, req, product_id):
        """Signals that an observation will start soon.
           Publishes a message to the 'alerts' channel of the form:
           capture-init:product_id
           The product_id matches that which was sent in the 
           ?configure request.
        """
        msg = "capture-init:{}".format(product_id)
        success = publish_to_redis(self.redis_server, REDIS_CHANNELS.alerts, msg)
        if success:
            return ("ok",)
        else:
            return ("fail", "Failed to publish to our local redis server")

    @request(Str())
    @return_reply()
    def request_capture_start(self, req, product_id):
        """Signals that an observation is starting now.
            Publishes a message to the 'alerts' channel of the form:
            capture-start:product_id
            The product_id matches that which was sent in the 
            ?configure request.
        """
        msg = "capture-start:{}".format(product_id)
        success = publish_to_redis(self.redis_server, REDIS_CHANNELS.alerts, msg)
        if success:
            return ("ok",)
        else:
            return ("fail", "Failed to publish to our local redis server")

    @request(Str())
    @return_reply()
    def request_capture_stop(self, req, product_id):
        """Signals that an observation is has stopped.
            Publishes a message to the 'alerts' channel of the form:
            capture-stop:product_id
            The product_id matches that which was sent in the 
            ?configure request.
        """
        msg = "capture-stop:{}".format(product_id)
        success = publish_to_redis(self.redis_server, REDIS_CHANNELS.alerts, msg)
        if success:
            return ("ok",)
        else:
            return ("fail", "Failed to publish to our local redis server")

    @request(Str())
    @return_reply()
    def request_capture_done(self, req, product_id):
        """Signals that an observation has finished.
            Publishes a message to the 'alerts' channel of the form:
            capture-done:product_id
            The product_id matches that which was sent in the 
            ?configure request.
        """
        msg = "capture-done:{}".format(product_id)
        success = publish_to_redis(self.redis_server, REDIS_CHANNELS.alerts, msg)
        if success:
            return ("ok",)
        else:
            return ("fail", "Failed to publish to our local redis server")

    @request(Str())
    @return_reply()
    def request_deconfigure(self, req, product_id):
        """Signals that the current subarray has been deconfigured (dismantled)
           and its associated components released for use in another subarray.

           This is the signal to deconfigure the BLUSE instance associated with 
           the current subarray (and which was created by the call to ?configure 
           with the corresponding product_id). 
           Note: CAM is expected to have sent a ?capture-done request before 
           deconfiguring. If BLUSE uses an instance of katportalclient to get 
           information from CAM for the current subarray, it should disconnect.

           Publishes a message to the 'alerts' channel of the form:
           deconfigure:product_id
           The product_id matches that which was sent in the ?configure request.

           Other backend processes (such as beamformer) should be notified 
           that their data streams are ending.
        """
        msg = "deconfigure:{}".format(product_id)
        success = publish_to_redis(self.redis_server, REDIS_CHANNELS.alerts, msg)
        if success:
            return ("ok",)
        else:
            return ("fail", "Failed to publish to our local redis server")

    def setup_sensors(self):
        # TODO: Need to re-look at this function.
        """
        @brief    Set up monitoring sensors.

        @note     The following sensors are made available on top of default sensors
                  implemented in AsynDeviceServer and its base classes.

                  device-status:      Reports the health status of the proxy and associated devices:
                                      Among other things report HW failure, SW failure and observation failure.
        """
        self._device_status = Sensor.discrete(
            "device-status",
            description="Health status of BLUSE",
            params=self.DEVICE_STATUSES,
            default="ok",
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._device_status)

        self._local_time_synced = Sensor.boolean(
            "local-time-synced",
            description="Indicates BLUSE is NTP syncronised.",
            default=True,  # TODO: implement actual NTP synchronization request
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._local_time_synced)

        self._version = Sensor.string(
            "version",
            description="Reports the current BLUSE version",
            default=str(self.VERSION_INFO[1:]).strip('()').replace(' ', '').replace(",", '.'),  # e.g. '1.0'
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._version)

    def request_halt(self, req, msg):
        """Halts the server, writes to the log, and exits the program.
        
        Returns:
            success : {'ok', 'fail'} (whether scheduling the halt succeeded).

        Examples:
            ?halt
            !halt ok

        TODO:
            - Call halt method on superclass to avoid copy paste
                Doing this caused an issue:
                    File "/Users/Eric/Berkeley/seti/packages/meerkat/lib/python2.7/site-packages/katcp/server.py", line 1102, in handle_request
                        assert (reply.mtype == Message.REPLY)
                    AttributeError: 'NoneType' object has no attribute 'mtype'
        """
        f = Future()
        @gen.coroutine
        def _halt():
            req.reply("ok")
            yield gen.moment
            self.stop(timeout=None)
            raise AsyncReply
        self.ioloop.add_callback(lambda: chain_future(_halt(), f))
        log.critical("HALTING SERVER!!!")
        # notify_slack("KATCP server at MeerKAT has halted. Might want to check that!")
        sys.exit(0)

    @request()
    @return_reply(Str())
    def request_find_alien(self, req):
        """Finds an alien.
        """
        return ("ok", R"""
.     .       .  .   . .   .   . .    +  .
  .     .  :     .    .. :. .___---------___.
       .  .   .    .  :.:. _".^ .^ ^.  '.. :"-_. .
    .  :       .  .  .:../:            . .^  :.:\.
        .   . :: +. :.:/: .   .    .        . . .:\
 .  :    .     . _ :::/:               .  ^ .  . .:\
  .. . .   . - : :.:./.                        .  .:\
  .      .     . :..|:                    .  .  ^. .:|
    .       . : : ..||        .                . . !:|
  .     . . . ::. ::\(                           . :)/
 .   .     : . : .:.|. ######              .#######::|
  :.. .  :-  : .:  ::|.#######           ..########:|
 .  .  .  ..  .  .. :\ ########          :######## :/
  .        .+ :: : -.:\ ########       . ########.:/
    .  .+   . . . . :.:\. #######       #######..:/
      :: . . . . ::.:..:.\           .   .   ..:/
   .   .   .  .. :  -::::.\.       | |     . .:/
      .  :  .  .  .-:.":.::.\             ..:/
 .      -.   . . . .: .:::.:.\.           .:/
.   .   .  :      : ....::_:..:\   ___.  :/
   .   .  .   .:. .. .  .: :.:.:\       :/
     +   .   .   : . ::. :.:. .:.|\  .:/|
     .         +   .  .  ...:: ..|  --.:|
.      . . .   .  .  . ... :..:.."(  ..)"
 .   .       .      :  .   .: ::/  .  .::\
        """)
