"""Diagnostic script for hashpipe-redis gateway.
"""

import redis
import time
import random 
import argparse
import sys

HPGDOMAIN = 'bluse'
N_NODES = 64
N_JOIN_LEAVE = 1000
GROUP_NAME = 'test'

def join_group(redis_server, host_list, group_name):
    """Tell specific hosts to join specific Hashpipe-Redis gateway groups. 
    """
    #Test leaving/joining functionality for Hashpipe-Redis gateway groups.
    if(host_list == 'all'):
        chan_name = '{}:///gateway'.format(HPGDOMAIN)
        n_listeners = pub_gateway_msg(redis_server, chan_name, 'join', group_name)
    else:
        n_listeners = 0
        for i in range(len(host_list)):
            chan_name = '{}://{}/gateway'.format(HPGDOMAIN, host_list[i])
            n_listeners += pub_gateway_msg(redis_server, chan_name, 'join', group_name)
    return n_listeners

def leave_group(redis_server, group_name):
    """Tell all hosts to leave specified group. 
    """
    chan_name = '{}:///gateway'.format(HPGDOMAIN)
    n_listeners = pub_gateway_msg(redis_server, chan_name, 'leave', group_name)
    return n_listeners

def pub_gateway_msg(redis_server, chan_name, msg_name, msg_val):
    """Format and publish a hashpipe-Redis gateway message. 
    
    Args:
        red_server: Redis server.
        chan_name (str): Name of channel to be published to. 
        msg_name (str): Name of key in status buffer.
        msg_val (str): Value associated with key.
    
    Returns:
        n_listeners (int): Number of listeners to gateway message. 

    """
    msg = '{}={}'.format(msg_name, msg_val)
    n_listeners = redis_server.publish(chan_name, msg)
    return n_listeners

def check_nodes(redis_server, host_list, hpgdomain):
    """Count number of nodes which are accesible via the Hashpipe-Redis
    gateway. Use this to show which nodes are inaccessible. 
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

def cli(args = sys.argv[0]):
    """CLI for antenna sensor retrieval.
    """
    usage = "{} [options]".format(args)
    description = 'Instruct processing nodes to Join and leave specified Hashpipe-Redis Gateway group'
    parser = argparse.ArgumentParser(prog = 'hr-gateway-diagnostics-', usage = usage,
        description = description)
    parser.add_argument('--group',
        type = str,
        help = 'Hashpipe-Redis Gateway group to join and leave.')
    if(len(sys.argv[1:])==0):
        parser.print_help()
        parser.exit()
    args = parser.parse_args()
    main(group = args.group)


def main(group):

    redis_server = redis.StrictRedis(decode_responses=True) #default host and port OK

    # build host list
    host_list = []
    for i in range(0, N_NODES):
        host_list.append('blpn{}'.format(i))

    # Join/leave in sequence     
    for i in range(0, N_JOIN_LEAVE):
        # Join test group
        print("Test {} of {}".format(i+1, N_JOIN_LEAVE))
        n_listeners = join_group(redis_server, host_list, group)
        if(n_listeners < N_NODES):
            print("    MISSING NODES: {}".format(N_NODES - n_listeners))
        else:
            print("    Joined group:  {} listeners".format(n_listeners))
        # Wait random time between 0.5 and 1 second
        time.sleep(random.uniform(0.001, 0.15)) 
        # Leave test group
        leave_group(redis_server, group)
        if(n_listeners < N_NODES):
            print("    MISSING NODES: {}".format(N_NODES - n_listeners))
        else:
            print("    Left group:    {} listeners".format(n_listeners))
        # Results:
        check_nodes(redis_server, host_list, HPGDOMAIN)
        #print("Test cycle {} of {} completed".format(i+1, N_JOIN_LEAVE))

if(__name__=='__main__'):
    cli() 
