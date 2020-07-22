import sys
import redis
import logging
try:
    from logger import log 
except(SystemError, ImportError):
    from meerkat_backend_interface.logger import log
try:
    from slack import WebClient as Client
except:
    from slackclient import SlackClient as Client

class SlackProxy(object):
    """Proxy for interfacing with Slack (for monitoring, debugging
    and alerts).

    Messages to be posted to Slack are sent to this proxy via Redis pub/sub
    channels. 

    To begin with, only posting to Slack is required. In future, two-way
    communication is envisaged (such as the ability to request information  
    from other backend processes from Slack, or the ability to control
    certain aspects of some of the process by sending them messages). 

    Calling start() starts a loop which listens for messages on a specified
    Redis channel - these messages are expected to conform to the following
    structure:
          
         <Slack channel>:<Slack message text> 

    The <Slack message text> is then posted to the <Slack channel>. The 
    current Slack bot must have the correct permissions and, if the channel 
    is a private channel, be a member of the channel. 
    """

    def __init__(self, redis_port, redis_channel, slack_token):
        """Initialise.
       
        Args:
            redis_port (int): port to listen on.
            redis_channel (str): channel on which to listen for
            messages to post to Slack).
            slack_token (str): Slack token (should be passed to 
            ../slack_proxy_start() by means of an environment variable.

        Returns:
            None
        """
        self.token = slack_token
        self.client = Client(token = slack_token)
        self.redis_server = redis.StrictRedis(port=redis_port, decode_responses=True)
        self.redis_channel = redis_channel
        # Get version number of environment
        self.python_version = sys.version_info

    def start(self):
        """Start the proxy.
        Messsages received via the Redis channel are expected to conform to 
        the following format:
        
            <Slack channel>:<Slack message text> 

        """
        log.info("Starting Slack Proxy")
        ps = self.redis_server.pubsub(ignore_subscribe_messages=True)
        ps.subscribe(self.redis_channel)
        try:
            for msg in ps.listen():
                msg = msg['data'].split(':', 1)
                slack_channel = msg[0]
                slack_msg = msg[1] 
                # Post message using either method depending on the python
                # version of the current environment.
                if(self.python_version[0] < 3):
                    self.legacy_post_msg(slack_msg, slack_channel)
                elif((self.python_version[0] == 3) & (self.python_version[1] > 5)):
                    self.post_msg(slack_msg, slack_channel)
                else:
                    log.warning("Python version unsupported; attempting anyway")
                    self.post_msg(slack_msg, slack_channel)
        except KeyboardInterrupt:
            log.info("Stopping Slack proxy")
            sys.exit(0)
        except Exception as e:
            log.error(e) 
            sys.exit(1)            

    def post_msg(self, message, slack_channel):
        """Post a message to a specified Slack channel.
    
        Args: 
             message (str): message to send to Slack.
             slack_channel (str): specified Slack channel (if a
             private channel, the corresponding Slack bot must 
             be a member of the channel first. 

        Returns:
            None
        """
        try:
            response = self.client.chat_postMessage(
                channel = slack_channel,
                text = message)
            log.debug(response)
            log.info("Posted message: {} to Slack channel: {}".format(message, slack_channel))
        except Exception as e:
            log.error("Failed to push to Slack")
            log.error(e)

    def legacy_post_msg(self, message, slack_channel):
        """Push a message to Slack (for use with the old
        client for python 2.7).
    
        Args: 
             message (str): message to send to Slack.
             slack_channel (str): specified Slack channel (if a
             private channel, the corresponding Slack bot must 
             be a member of the channel first.

        Returns:
            None
        """
        try:
            response = self.client.api_call("chat.postMessage",
                channel = slack_channel,
                text = message)
            log.debug(response)
            log.info("Posted message: {} to Slack channel: {}".format(message, slack_channel))
        except Exception as e:
            log.error("Failed to push to Slack")
            log.error(e)

