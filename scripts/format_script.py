import numpy as np
from telstate_interface import TelstateInterface
import redis
import pickle
import time

TelInt = TelstateInterface()
cal_K, cal_G, cal_B, cal_all, timestamp = TelInt.query_telstate('10.98.2.128:31829', '/home/danielc/')

red = redis.StrictRedis(port='6379', decode_responses=True)

print(cal_all)
test2 = cal_all

def cal_array(cals):
    """Format calibration solutions into a multidimensional array. 
    
    Args:
        cals (dict): dictionary of calibration solutions as returned from
                     Telstate.

    Returns:
        cal_mat (numpy matrix): complex float values of dimensions:
                                (pol, nchans, nants)
                                Antennas are sorted by number.
                                H-pol is first.  
    """
    # Determine number of channels:    
    ant_keys = list(cals.keys())
    try:
        nchans = len(cals[ant_keys[0]])
    except:
        # if a single value for each antenna
        nchans = 1
    # Determine number of antennas:
    nants = int(len(ant_keys)/2) # since two polarisations
    # Ordered list of antenna number:
    ant_n = []
    for key in ant_keys:
        ant_n.append(int(key[1:4]))
    ant_n = np.unique(np.array(ant_n))
    ant_n = np.sort(ant_n)
    # Fill multidimensional array:
    result_array = np.zeros((2, nchans, nants), dtype=np.complex)
    for i in range(len(ant_n)):
       # hpol:
       ant_name = 'm{}h'.format(str(ant_n[i]).zfill(3))
       result_array[0, :, i] = cals[ant_name]
       # vpol:
       ant_name = 'm{}v'.format(str(ant_n[i]).zfill(3))
       result_array[1, :, i] = cals[ant_name]
    return result_array

def format_cals(product_id, cal_K, cal_G, cal_B, cal_all, nants, ants, nchans, timestamp):
    """Write calibration solutions into a Redis hash under the correct key. 
       Calibration data is serialised before saving. 
    """
    # Serialise calibration data
    cal_K = pickle.dumps(cal_array(cal_K))
    cal_G = pickle.dumps(cal_array(cal_G))
    cal_B = pickle.dumps(cal_array(cal_B))
    cal_all = pickle.dumps(cal_array(cal_all))
    # Save current calibration session to Redis
    hash_key = "{}:cal_solutions:{}".format(product_id, timestamp)
    hash_dict = {"cal_K":cal_K, "cal_G":cal_G, "cal_B":cal_B, "cal_all":cal_all,
                "nants":nants, "antenna_list":str(ants), "nchan":nchans} 
    red.hmset(hash_key, hash_dict)    
    # Save to index (zset)
    index_name = "{}:cal_solutions:index".format(product_id)
    index_score = int(time.time())
    red.zadd(index_name, {hash_key:index_score}) 



ant_key = 'array_1:antennas'
ant_list = red.lrange(ant_key, 0, red.llen(ant_key))

format_cals('array_1', cal_K, cal_G, cal_B, cal_all, 63, ant_list, 4096, timestamp)


