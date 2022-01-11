"""Diagnostic script for hashpipe-redis gateway.
"""

import redis
import time
import random

HPGDOMAIN   = 'bluse'
N_NODES = 64
N_JOIN_LEAVE = 100
GROUP_NAME = 'test'

def join_group(redis_server, host_list, group_name):
    """Tell specific hosts to join specific Hashpipe-Redis gateway groups. 
    """
    #Test leaving/joining functionality for Hashpipe-Redis gateway groups.
    for i in range(len(host_list)):
        chan_name = '{}://{}/gateway'.format(HPGDOMAIN, host_list[i])
        pub_gateway_msg(redis_server, chan_name, 'join', group_name, True)

def leave_group(redis_server, group_name):
    """Tell all hosts to leave specified group. 
    """
    chan_name = '{}:///gateway'.format(HPGDOMAIN)
    pub_gateway_msg(redis_server, chan_name, 'leave', group_name, True)

def pub_gateway_msg(redis_server, chan_name, msg_name, msg_val, write):
    """Format and publish a hashpipe-Redis gateway message. Save messages
    in a Redis hash for later use by reconfig tool. 
    
    Args:
        red_server: Redis server.
        chan_name (str): Name of channel to be published to. 
        msg_name (str): Name of key in status buffer.
        msg_val (str): Value associated with key.
        write (bool): If true, also write message to Redis database.
    """
    msg = '{}={}'.format(msg_name, msg_val)
    redis_server.publish(chan_name, msg)
    # save hash of most recent messages
    if(write):
        redis_server.hset(chan_name, msg_name, msg_val)
        #print('Wrote {} for channel {} to Redis'.format(msg, chan_name))
    #print('Published {} to channel {}'.format(msg, chan_name))

def check_nodes(redis_server, host_list, hpgdomain):
    """Count number of nodes which are accesible via the Hashpipe-Redis
    gateway.
    """
    print("Counting accessible hosts:")
    n_accessible = 0
    for host in host_list:
        host_key = "{}://{}/0/status".format(hpgdomain, host)
        host_status = redis_server.hgetall(host_key)
        if(len(host_status) > 0):
            n_accessible += 1
        else:
            print("    {} is inaccessible".format(host))
    print("Accessible hosts: {}".format(n_accessible))

if(__name__ == '__main__'):

    redis_server = redis.StrictRedis(decode_responses=True) #default host and port OK

    # build host list
    host_list = []
    for i in range(0, N_NODES):
        host_list.append('blpn{}'.format(i))

    # Join/leave in sequence     
    for i in range(0, N_JOIN_LEAVE):
        # Join test group
        join_group(redis_server, host_list, GROUP_NAME)
        # Wait random time between 0.5 and 1 second
        time.sleep(random.uniform(0.3, 0.6)) 
        # Leave test group
        leave_group(redis_server, GROUP_NAME)
        # Results:
        check_nodes(redis_server, host_list, HPGDOMAIN)
        print("Test cycle {} of {} completed".format(i+1, N_JOIN_LEAVE)) 
