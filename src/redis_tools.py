import logging
import redis

log = logging.getLogger("BLUSE.interface")

class REDIS_CHANNELS:
    """The redis channels that may be published to"""
    alerts = "alerts"

def write_pair_redis(server, key, value):
    """Creates a key-value pair self.redis_server's redis-server.

    Args:
        server (redis.StrictRedis) a redis-py redis server object
        key (str): the key of the key-value pair
        value (str): the value of the key-value pair
    
    Returns:
        None... but logs either an 'debug' or 'error' message
    
    Examples:
        >>> server = BLBackendInterface('localhost', 5000)
        >>> server._write_to_redis("aliens:found", "yes")
    """
    try:
        server.set(key, value)
        log.debug("Created redis key/value: {} --> {}".format(key, value))
    except:
        log.error("Failed to create redis key/value pair")

def write_list_redis(server, key, values):
    """Creates a new list and rpushes values to it

        If a list already exists at the given key, then
        delete it and rpush values to a new empty list
        
        Args:
            server (redis.StrictRedis) a redis-py redis server object
            key (str): key identifying the list
            values (list): list of values to rpush to redis list

    """
    if server.exists(key):
        server.delete(key)
    try:
        server.rpush(key, *values)
        log.debug("Pushed to list: {} --> {}".format(key, values))
    except:
        log.error("Failed to rpush to {}".format(channel))

def publish_to_redis(server, channel, message):
    """Publishes a message to a channel in self.redis_server's redis-server.

    Args:
        server (redis.StrictRedis) a redis-py redis server object
        channel (str): the name of the channel to publish to
        message (str): the message to be published
    
    Returns:
        None... but logs either an 'debug' or 'error' message
    
    Examples:
        >>> server = BLBackendInterface('localhost', 5000)
        >>> server._publish_to_redis("alerts", "Found aliens!!!")
    """
    try:
        server.publish(channel, message)
        log.debug("Published to {} --> {}".format(channel, message))
    except:
        log.error("Failed to publish to {}".format(channel))