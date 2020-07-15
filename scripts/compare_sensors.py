"""
Script to compare a sensor .csv file obtained from the katportal GUI
(which contains historical sensor values and timestamps) with the 
BLUSE logs and the KATGUI progress logs. 
Intended for debugging purposes.

See also scrape_history_from_bluse_logs.rb - used to scrape 
logs for the antennas and target sensors and save their history
to Redis.

Potential sensor log sources:

    SENSOR_FILE    .csv downloaded from sensor history (from
                   portal.mkat.karoo.kat.ac.za)

    LOG_FILE       Derived from BLUSE katportal_server logs.

    PR_LOG_FILE    Schedule block progress log file (also 
                   from portal.mkat.karoo.kat.ac.za)

    ASYNC_FILE     Logs from independently running script
                   that connects to katportalclient separately
                   for debugging purposes.
                   (scripts/subscribe_async.py)

    MON_FILE       Logs from independent process that also
                   connects to katportalclient.
"""

from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib as mpl
import pickle
import numpy as np
import ast

SENSOR_FILE = 'subarray_1_observation_activity.csv' 
LOG_FILE = 'katportal.txt'
PROGRESS_LOG_FILE = 'sb_joined.log'
MON_FILE = 'mon_logs.txt'
ASYNC_FILE = 'subscribe_async.txt'

KEYSTRING = 'observation_activity'

# Format matplotlib fonts:
font = {'family' : 'normal',
        'weight' : 'normal',
        'size'   : 22}
mpl.rc('font', **font)

def scrape_katportal_log(logfile, keystring):
    """Retrieve timestamps and values for a katportal log entry containing 
       a particular string.

       Examples of logfile lines:

           [2020-06-19 19:31:43,347 - INFO - katportal_server.py:108] 
           {'msg_channel': 'array_1:subarray_1_observation_activity', 
           'msg_pattern': 'array_1:*', 
           'msg_data': {'received_timestamp': 1592587903.256694, 
           'status': 'nominal', 'value': 'track', 
           'name': 'subarray_1_observation_activity', 
           'timestamp': 1592587903.255768}}

           [2020-07-10 15:26:08,522 - DEBUG - client.py:640] 
           Message received: 
           {'id': 'redis-pubsub', 
           'result': {'msg_data': {
           'received_timestamp': 1594387568.351311, 
           'name': 'subarray_1_observation_activity', 
           'timestamp': 1594387568.349612, 
           'value': 'stop', 
           'status': 'nominal'}, 
           'msg_pattern': 'array_1_cac664b8-ec46-4182-945b-a54652f07fda:*', 
           'msg_channel': 
           'array_1_cac664b8-ec46-4182-945b-a54652f07fda:subarray_1_observation_activity'}}

       Searches specifically for entries which contain sensor 
       values as well (containing the string: '-->')

       Args:
           logfile (str): Path to logfile.
           keystring (str): String which identifies which line 
           to retrieve.

       Returns:
           values (list): List of extracted sensor values.
           times (list): List of timestamps (datetime objects).
    """
    values = []
    times = []
    with open(logfile, 'r') as f:
        for line in f:
            # Make sure we are getting the correct log message. 
            # Again highly specific to the BLUSE logging formats.
            if((keystring in line) & 
                ('client.py:640' in line)):
                try:
                    time, value = extract_log_line(line)
                    values.append(value)
                    times.append(time)
                except:
                    # If accidentally got a malformed or incorrect log 
                    # message, continue to the next valid line. 
                    continue
    return(values, times)

def scrape_async_log(logfile):
    """Extract time and state of a single sensor from the async logs 
    (from scripts/subscribe_async.py).

    Example of a log entry:
        [2020-07-10 13:44:14,258 - INFO - subscribe_async.py:70] 
        subarray_1_observation_activity:track

    Args:
        logfile (str): path to logfile.

    Returns:
        values (list): List of extracted sensor values.
        times (list): List of timestamps (datetime objects). 
    """
    values = []
    times = []
    with open(logfile, 'r') as f:
        for line in f:
            value = line.rsplit(':', 1)[1].strip()
            # Get timestring:
            time_info = line.split(']')[0]
            time_info = time_info.split(' - ')[0]
            # Remove leading '['
            timestring = time_info[1:]
            time = datetime.strptime(timestring, '%Y-%m-%d %H:%M:%S,%f')
            # Change to UTC:
            time = time - timedelta(hours=2)
            values.append(value)
            times.append(time)
    return(values, times)         

def extract_log_line(line):
    """Extract the date, time and sensor value from a particular
       logfile line.
       Highly specific to BLUSE katportal/katcp/coordinator logfiles.

       Args:
           line (str): Line retrieved from logfile. It is actually 
           a stringified dictionary.

       Returns:
           time (datetime object)
           sensor_value (str): sensor value extracted from the line.

    """   
    # Get the stringified dictionary portion of the log line:
    sensor_info = line.split(']')[1].strip()
    sensor_info = sensor_info.split(':', 1)[1].strip()
    sensor_info = "{}".format(sensor_info)
    try:
        sensor_info = ast.literal_eval(sensor_info)
        sensor_value = sensor_info["result"]["msg_data"]["value"]
        # Get the date and time:
        timestring = line.split(' - ')[0]
        # Remove leading '['.
        timestring = timestring[1:]
        time = datetime.strptime(timestring, '%Y-%m-%d %H:%M:%S,%f')
        # Change to UTC:
        time = time - timedelta(hours=2)
        return(time, sensor_value)
    except:
        print("Could not read line")

def read_sensor_csv(sensor_file):
    """Read in a sensor .csv file from portal.mkat.karoo.kat.ac.za

       Example of a sensor file entry:
           2020-06-19 10:55:25,nominal,track

       Args:
           sensor_file (str): Filepath to sensor log file.

       Returns:
           times (list): List of datetime objects.
           values (list): List of sensor values.
    """
    values = []
    times = []
    with open(sensor_file, 'r') as f:
        # Skip header:
        f.readline()
        for line in f:
            entry = line.split(',')
            value = entry[-1].strip()
            time = datetime.strptime(entry[0],  '%Y-%m-%d %H:%M:%S')
            values.append(value)
            times.append(time)
    return(values, times)

def read_track_logs(logfile):
    """Read in progress logs from katgui and format for plotting.
       Logs are split across two lines as follows:

       Examples of file entries:
           2020-06-19 14:56:36.004Z INFO     tracking target
           2020-06-19 14:56:36.005Z INFO     target tracked for 0.0 seconds

       Observations can also be terminated as follows:
           2020-07-10 13:25:58.253Z WARNING  Session interrupted by exception (KeyboardInterrupt)

       Args:
           logfile (str): Filepath to sensor log file.

       Returns:
           times (list): List of datetime objects.
           values (list): List of sensor values.
    """
    values = []
    times = []
    with open(logfile, 'r') as f:
        for line in f:
            # The following completely dependent on this particular 
            # log format.
            if('tracking target' in line):
                # Find start time of initiating the track:
                entry = line.split(' ')
                timestring = '{} {}'.format(entry[0], entry[1][:-1])
                time = datetime.strptime(timestring, '%Y-%m-%d %H:%M:%S.%f')
                values.append('track')
                times.append(time)
                # Find the length of the track by looking at the next line:
                next_line = f.readline()
                next_entry = next_line.split(' ')
                stop_timestring = '{} {}'.format(next_entry[0], next_entry[1][:-1])
                stop_time = datetime.strptime(stop_timestring, '%Y-%m-%d %H:%M:%S.%f')
                # Now, upon stopping, set to 'unknown'?
                times.append(stop_time)
                values.append('unknown')
    return(values, times)

def plot_values(s_vals, s_times, l_vals, l_times, pr_vals, pr_times, a_vals, a_times):
    """Plot the values against one another with timestamps.

       Args:
           s_vals (list): List of KATGUI sensor graph values.
           s_times (list): List of associated datetime objects. 
           l_vals (list): List of logged katportalclient 
           sensor values.
           l_times (list): List of associated datetime objects.
           pr_vals (list): List of progress log sensor values 
           (from KATGUI).
           pr_times (list): List of associated datetime objects.  
           a_vals (list): List of async script sensor values.
           a_times (list): List of associated datetime objects.

       Returns:
           None
    """
    
    # Figure formatting
    fig, ax = plt.subplots()
    fig.subplots_adjust(bottom=0.25)
    fig.subplots_adjust(left=0.15)

    # log dates to get ranges for plotting format.
    l_dates = mpl.dates.date2num(l_times)
    # It appears that in order to set the correct order of ytick 
    # items, one must actually plot and remove something to 'force'  
    # the correct order!
    states = ['unknown', 'idle', 'stop', 'slew', 'track']
    temp, = ax.plot(np.arange(np.min(l_dates), 
        np.min(l_dates) + len(states)), states)
    temp.remove()

    # Sensor values as visible in KATGUI sensor graph:
    s_dates = mpl.dates.date2num(s_times)
    plt.step(s_dates, s_vals, label = 'KATGUI sensor graph', 
        linestyle = '--', color = 'green', where = 'post')

    # Progress log sensor values from KATGUI observations:
    pr_dates = mpl.dates.date2num(pr_times)
    plt.step(pr_dates, pr_vals, label = 'SB progress log', 
        linestyle = ':', color = 'b', where = 'post')

    # Sensor values as actually received by the backend:
    plt.step(l_dates, l_vals, label = 'BLUSE (received)', 
        linestyle = '-.', color = 'k', where = 'post')

    # Sensor values as received by the async script:
    a_dates = mpl.dates.date2num(a_times)
    plt.step(a_dates, a_vals, label = 'Async (received)', 
        linestyle = '--', color = 'r', where = 'post')

    # Find all possible sensor states
    states = set(s_vals).union(set(l_vals)).union(set(pr_vals))
    print('States: {}'.format(states))

    # Plotting formatting
    ax.xaxis.set_major_formatter(mpl.dates.DateFormatter("%H:%M:%S"))
    xlocator = mpl.dates.SecondLocator(interval = 30)
    # Slightly hacky, but this is a script after all:
    xlocator.MAXTICKS = 12900 
    ax.xaxis.set_major_locator(xlocator)
    plt.xticks(rotation=90)
    plt.legend(loc = 'lower left')
    plt.xlim(left = l_dates[0], right = l_dates[-1])
    plt.title('Sensor: observation-activity')
    plt.xlabel('Time [UTC]', labelpad=5)
    plt.ylabel('Sensor Value', labelpad=5)
    plt.show()

if(__name__ == '__main__'):

    # Load logs
    
    pr_vals, pr_times = read_track_logs(PROGRESS_LOG_FILE)
    s_vals, s_times = read_sensor_csv(SENSOR_FILE)
    l_vals, l_times = scrape_katportal_log(LOG_FILE, KEYSTRING)
    a_vals, a_times = scrape_async_log(ASYNC_FILE)

    # Load pre-processed logs if need be:
    # with open('l_vals.txt', 'rb') as f:
    #     l_vals = pickle.load(f)
    # with open('l_times.txt', 'rb') as f:
    #     l_times = pickle.load(f)

    plot_values(s_vals, s_times, l_vals, l_times, pr_vals, pr_times, a_vals, a_times)
