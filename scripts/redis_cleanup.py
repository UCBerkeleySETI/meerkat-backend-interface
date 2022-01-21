"""Script for cleaning up entries in Redis (for example, updating entry 
formats, or retroactively fixing data issues). 
"""

import redis
import pickle
import numpy as np

def replace_with_bytes(redis_server, start_ts, stop_ts, zset_index):
    """This function retrieves all calibration solution entries between 
    start_ts and stop_ts (UTC timestamps) in the sorted set zset_index,
    and replaces the pickled calibration solution matrices with bytes.

    Args:
        redis_server: current Redis server (default port, host).
        start_ts (int): Lower (older) end of time-range in question 
                        (UTC in seconds).
        stop_ts (int): Upper (more recent) end of time-range in question
                       (UTC in seconds). 
        zset_index (str): Name of the index of keys (sorted set with score
                          given by the UTC timestamp.  
    """ 
    hkeys = select_keys(redis_server, start_ts, stop_ts, zset_index)

    for hkey in hkeys:
        hkey = hkey.decode("utf-8")
        print('Cleaning up {}'.format(hkey))        
        # Rename key so old version is kept:
        # NOTE: command COPY requires Redis >=6.2.0, therefore alternative
        # workaround approach is taken here.
        hkey_to_copy = redis_server.dump(hkey)
        redis_server.restore("{}_pickle".format(hkey), 0, hkey_to_copy) 

        # Convert pickled calibration data to bytes
        cal_G = pickle_to_bytes(redis_server, hkey, 'cal_G')
        cal_B = pickle_to_bytes(redis_server, hkey, 'cal_B')
        cal_K = pickle_to_bytes(redis_server, hkey, 'cal_K')
        cal_all = pickle_to_bytes(redis_server, hkey, 'cal_all')

        # Replace keys in set
        redis_server.hset(hkey, 'cal_G', cal_G)  
        redis_server.hset(hkey, 'cal_B', cal_B)  
        redis_server.hset(hkey, 'cal_K', cal_K)  
        redis_server.hset(hkey, 'cal_all', cal_all)  

def pickle_to_bytes(redis_server, hkey, key):
    """Convert pickled matrix stored in entry under `key`
    in Redis hash under `hkey` to bytes. 

    Args:
        redis_server: Redis server containing hash under `hkey`.
        hkey (str): Key under which Redis hash is stored. 
        key (str): Key under which the pickled matrix is stored. 
    
    Returns:
        key_bytes: Matrix converted to bytes. 
    """
    key_pickled = redis_server.hget(hkey, key)
    key = pickle.loads(key_pickled)
    key_bytes = key.tobytes()
    return key_bytes

def select_keys(redis_server, start_time, stop_time, zset):
    """Select desired keys from the specified sorted set.
    NOTE: Using ZRANGEBYSCORE here instead of ZRANGE with the BYSCORE
    argument, since local Redis version <6.2.0. 
    """
    return redis_server.zrangebyscore(zset, start_time, stop_time)


if(__name__ == '__main__'):
    
    redis_server = redis.StrictRedis()
    # Convert pickled Numpy matrices to bytes for given range:
    replace_with_bytes(redis_server, 0, 1642169854, 'array_1:cal_solutions:index'):
    replace_with_bytes(redis_server, 0, 1642169854, 'array_2:cal_solutions:index'):
    replace_with_bytes(redis_server, 0, 1642169854, 'array_3:cal_solutions:index'):

