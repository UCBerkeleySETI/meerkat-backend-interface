"""
Script to compare a sensor .csv file obtained from the katportal GUI
(which contains historical sensor values and timestamps) with the 
BLUSE logs and the KATGUI progress logs. 
Intended for debugging purposes.

See also scrape_history_from_bluse_logs.rb - used to scrape 
logs for the antennas and target sensors and save their history
to Redis.
"""

from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib as mpl
import pickle
import numpy as np
import ast

SENSOR_FILE = 'subarray_1_observation_activity_A.csv' 
#SENSOR_FILE = '/home/danielc/sensor_file_A.csv'
LOG_FILE = '/var/log/bluse/katportal/katportal.err'
PROGRESS_LOG_FILE = 'track_logs_2020-06-19.log'
KEYSTRING = 'observation_activity'

# Format matplotlib fonts:
font = {'family' : 'normal',
        'weight' : 'normal',
        'size'   : 22}
mpl.rc('font', **font)

def scrape_log(logfile, keystring):
    """Retrieve timestamps and values for a log entry containing 
       a particular string.

       Example of a logfile line:
           [2020-06-19 19:31:43,347 - INFO - katportal_server.py:108] 
           {'msg_channel': 'array_1:subarray_1_observation_activity', 
           'msg_pattern': 'array_1:*', 
           'msg_data': {'received_timestamp': 1592587903.256694, 
           'status': 'nominal', 'value': 'track', 
           'name': 'subarray_1_observation_activity', 
           'timestamp': 1592587903.255768}}

       Searches specifically for entries which contain sensor 
       values as well (containing the string: '-->')

       Args:
           logfile (str): Path to katportal_server logfile.
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
                ('katportal_server.py:108' in line)):
                try:
                    time, value = extract_log_line(line)
                    values.append(value)
                    times.append(time)
                except:
                    # If accidentally got a malformed or incorrect log 
                    # message, continue to the next valid line. 
                    continue
    
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
    sensor_info = ast.literal_eval(sensor_info)["msg_data"]
    sensor_value = sensor_info["value"]
    # Get the date and time:
    timestring = line.split(' - ')[0]
    # Remove leading '['.
    timestring = timestring[1:]
    time = datetime.strptime(timestring, '%Y-%m-%d %H:%M:%S,%f')
    # Change to UTC:
    time = time - timedelta(hours=2)
    
    return(time, sensor_value)

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

       Example of a file entry:
           Test 1 sb0027.log 2020-06-19 14:56:36.004Z INFO     tracking target
           Test 1 sb0027.log 2020-06-19 14:56:36.005Z INFO     target tracked for 0.0 seconds

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
                timestring = '{} {}'.format(entry[3], entry[4][:-1])
                time = datetime.strptime(timestring, '%Y-%m-%d %H:%M:%S.%f')
                # Find the length of the track by looking at the next line:
                next_line = f.readline()
                next_entry = next_line.split(' ')
                duration = next_entry[-2]
                # If track of length 0, skip
                if(float(duration) == 0):
                    continue
                values.append('track')
                times.append(time)
                # Now, after duration, set to 'unknown'?
                times.append(time + timedelta(0, float(duration)))
                values.append('unknown')
    
    return(values, times)

def plot_values(s_vals, s_times, l_vals, l_times, pr_vals, pr_times):
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
    #l_vals, l_times = scrape_log(LOG_FILE, KEYSTRING)

    # Load pre-processed logs if need be:
    with open('l_vals.txt', 'rb') as f:
        l_vals = pickle.load(f)
    with open('l_times.txt', 'rb') as f:
        l_times = pickle.load(f)

    plot_values(s_vals, s_times, l_vals, l_times, pr_vals, pr_times)
