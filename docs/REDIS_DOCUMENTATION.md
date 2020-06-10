# Redis Documentation

*The information made available by this interface and the format within which it is stored in redis*

The [redis-py](https://github.com/andymccurdy/redis-py) python module is used in the `meerkat-backend-interface`.

# Key-Value Pairs
*Key-value pairs created by the different modules.*

## Key-value pairs created from information in CAM `configure` message:

| Key                          | Type  | Description                                                                                                                   | Created By       | Stage Created       |   
|:-----------------------------|:------|:------------------------------------------------------------------------------------------------------------------------------|:-----------------|:--------------------|
| `current:obs:id`             | str   | The `product_id` is a temporary identifier of the subarray currently active. Identical across repeated subarray activations.  | `katcp_server`   | `configure`         |
| `[product_id]:timestamp`     | str   | Unix time at which the `?configure` request was processed. Generated with `time.time()`.                                      | `katcp_server`   | `configure`         |
| `[product_id]:antennas`      | list  | List of antenna component names for the current subarray.                                                                     | `katcp_server`   | `configure`         |
| `[product_id]:n_channels`    | str   | Number of frequency channels.                                                                                                 | `katcp_server`   | `configure`         |
| `[product_id]:proxy_name`    | str   | CAM name for the instance of the BLUSE data proxy that is being configured. Eg “BLUSE_3”.                                     | `katcp_server`   | `configure`         |
| `[product_id]:cam:url`       | str   | URL used by `katportal_server` to query additional metadata for the subarray associated with the given product id.            | `katcp_server`   | `configure`         |
| `[product_id]:streams`       | str   | A json-formatted string for a python dict containing URLs for the different types of raw data. See example below.             | `katcp_server`   | `configure`         |
| `[product_id]:[sensor_name]` | str   | Sensor information stored in Redis. See example below.                                                                        | `katcp_server`   | `configure`         |
| `[product_id]:cbf_prefix`    | str   | Current CBF sensor prefix extracted from CAM message. `i0` for `katportalclient` < 0.2.0 and `wide` for later versions.       | `katcp_server`   | `configure`         |

### Example of `[product_id]:streams` formatting:

```
{
    "cam.http":
        {"camdata":"http://monctl.devnmk.camlab.kat.ac.za/api/client/2"},
    "stream_type1":
        {
            "stream_name1":"stream_address1",
            "stream_name2":"stream_address2"
        }
    }
    "stream_type2":
        {
            "stream_name1":"stream_address1",
            "stream_name2":"stream_address2"
        }
    }
}
```
The dict may contain:
* One CAM stream, with type cam.http. The camdata stream provides the connection string for katportalclient (for the subarray that this BLUSE instance is being configured on). This is the same as `[product_id]:cam:url`.
* One F-engine stream, with type:  `cbf.antenna_channelised_voltage.`
* One X-engine stream, with type:  `cbf.baseline_correlation_products.`
* Two beam streams, with type: `cbf.tied_array_channelised_voltage`.  The stream names ending in x are horizontally polarised, and those ending in y are vertically polarised

### Example of `[product_id]:[sensor_name]` formatting:

```
{
    'status': u'nominal', 
    'timestamp': 1533319620.245345, 
    'value': u'PKS 0408-65, radec bfcal single_accumulation, 4:08:20.38, -65:45:09.1, (800.0 8400.0 -3.708 3.807 -0.7202)', 
    'value_timestamp': 1533291480.096976
}
```

## Sensor key-value pairs obtained from the `katportalclient`:

These sensor values are acquired via `katportalclient`. Note that for certain sensors, websocket subscriptions are made at particular stages of an observation. After a websocket subscription has been made, the stored values for these sensors are updated asynchronously whenever they change.

| Key                                                                                                         | Type  | Description                                                                               | Created By          | Stage Created                    |
|:------------------------------------------------------------------------------------------------------------|:------|:------------------------------------------------------------------------------------------|:--------------------|:---------------------------------|
| `[product_id]:[antenna]_target`                                                                             | str   | Requested target (description and RA, Dec)                                                | `katportal_server`  | Subscription on `capture-init`   |
| `[product_id]:subarray_[subarray_num]_pool_resources`                                                       | str   | List of components included in subarray (Eg `[bluse_1, m001, m002, m012, cbf_1, sdp_1]`)  | `katportal_server`  | `configure`                      |
| `[product_id]:subarray_[subarray_num]_observation_activity`                                                 | str   | Status of observation (slew, track, etc).                                                 | `katportal_server`  | Subscription on `capture-start`  |
| `[product_id]:[cbf_name]_[subarray_num]_[cbf_prefix] _antenna_channelised_voltage_n_chans_per_substream`    | str   | Number of channels per SPEAD2 substream.                                                  | `katportal_server`  | `configure`                      |
| `[product_id]:[cbf_name]_[subarray_num]_[cbf_prefix] _tied_array_channelised_voltage_0x_spectra_per_heap`   | str   | Number of spectra per SPEAD2 heap.                                                        | `katportal_server`  | `configure`                      |
| `[product_id]:[cbf_name]_[subarray_num]_[cbf_prefix]_sync_time`                                             | str   | Supplied UNIX synchronisation time (from CBF).                                            | `katportal_server`  | `configure`                      |
| `[product_id]:[cbf_name]_[subarray_num]_[cbf_prefix] _antenna_channelised_voltage_n_samples_between_spectra`| str   | Number of samples from which each spectra is generated.                                   | `katportal_server`  | `configure`                      |
| `[product_id]:schedule_blocks`                                                                              | str   | List of approved schedule blocks for the current subarray.                                | `katportal_server`  | `capture-init`                   |
| `[product_id]:subarray_[subarray_num]_[cbf_prefix]_antenna_channelised_voltage_centre_frequency`            | str   | Observation centre frequency.                                                             | `katportal_server`  | `configure`                      |
| `[product_id]:subarray_[subarray_num]_[cbf_prefix]_antenna_channelised_voltage_input_data_suspect`          | str   | Data-suspect mask for each F-engine, both pols.                                           | `katportal_server`  | Subscription on `capture-start`  |
| `[product_id]:[cbf_name]]_[cbf_prefix]_input_labelling`                                                     | str   | Mapping from antenna name to F-engine ID.                                                 | `katportal_server`  | `configure`                      |



# Messages

## Internal Channels: `alerts` and `sensor_alerts`

| Message                             | Description                                                                                        | Channel         | Publisher(s)       | Subscriber(s)                      |
|:------------------------------------|:---------------------------------------------------------------------------------------------------|:----------------|:-------------------|:-----------------------------------|
| `configure:[product_id]`            | Published when a configure request is received, indicating that a subarray has been built.         | `alerts`        | `katcp_server`     | `katportal_server`, `coordinator`  |
| `conf_complete:[product_id]`        | Published once all sensor values required at subarray configuration time have been acquired        | `alerts`        | `katportal_server` | `coordinator`                      |
| `capture-init:[product_id]`         | Published when a capture-init request is received, indicating that a program block is starting.    | `alerts`        | `katcp_server`     | `katportal_server`, `coordinator`  |
| `capture-start:[product_id]`        | Published when a capture-start request is received, indicating the start of an observation.        | `alerts`        | `katcp_server`     | `katportal_server`, `coordinator`  |
| `capture-stop:[product_id]`         | Published when a capture-stop request is received, indicating the end of an observation.           | `alerts`        | `katcp_server`     | `katportal_server`, `coordinator`  |
| `capture-done:[product_id]`         | Published when a capture-done request is received, indicating the end of a program block.          | `alerts`        | `katcp_server`     | `katportal_server`, `coordinator`  |
| `deconfigure:[product_id]`          | Published when a deconfigure request is received, indicating that a subarray has been broken down. | `alerts`        | `katcp_server`     | `katportal_server`, `coordinator`  |
| `[product_id]:data_suspect:[mask]`  | Binary mask indicating data-suspect for each polarisation of each F-engine. 1 = data is suspect.   | `sensor_alerts` | `katportal_server` | `coordinator`                      |
| `[product_id]:target:[value]`       | Current target under observation.                                                                  | `sensor_alerts` | `katportal_server` | `coordinator`                      |
| `tracking:[product_id]`             | Published when the subarray begins tracking a target.                                              | `sensor_alerts` | `katportal_server` | `coordinator`                      |
| `not-tracking:[product_id]`         | Published when the subarray ceases to track a target.                                              | `sensor_alerts` | `katportal_server` | `coordinator`                      |


## Hashpipe-Redis Gateway

SPEAD stream addresses are published along with other information to individual channels, one each per processing instance. An example of a channel: `bluse://blpn48/0/set`. The messages published to this channel are formatted for use with the hashpipe-redis gateway. Messages published to the channel `[HPGDOMAIN]:///set ` are received by all processing instances.

| Message                           | Description                                                                                      | Channel                                 |
|:----------------------------------|:-------------------------------------------------------------------------------------------------|:----------------------------------------|
| `NSTRM=[num streams]`             | Number of streams apportioned to each processing instance.                                       | `[HPGDOMAIN]://[hashpipe_instance]/set` | 
| `DESTIP=[SPEAD addresses]`        | IP addresses for SPEAD streams.                                                                  | `[HPGDOMAIN]://[hashpipe_instance]/set` |
| `SCHAN=[first starting channel]`  | Absolute starting channel number for a particular instance.                                      | `[HPGDOMAIN]://[hashpipe_instance]/set` |
| `DESTIP=[port]`                   | Port number for SPEAD streams.                                                                   | `[HPGDOMAIN]:///set`                    |
| `DESTIP=0.0.0.0`                  | Message to unsubscribe to streams (sent on deconfigure).                                         | `[HPGDOMAIN]:///set`                    |  
| `FENCHAN=[num channels]`          | Total number of channels received on configure.                                                  | `[HPGDOMAIN]:///set`                    |
| `FENSTRM=[num streams]`           | Total number of streams received on configure.                                                   | `[HPGDOMAIN]:///set`                    |
| `HNCHAN=[channels per substream]` | Number of channels per substream.                                                                | `[HPGDOMAIN]:///set`                    |
| `HNTIME=[spectra per heap]`       | Number of spectra per heap.                                                                      | `[HPGDOMAIN]:///set`                    |
| `SYNCTIME=[sync time]`            | UNIX sync time, obtained when `configure` is published to the `alerts` channel.                  | `[HPGDOMAIN]:///set`                    |
| `HCLOCKS=[ADC samples per heap]`  | Number of ADC samples per heap.                                                                  | `[HPGDOMAIN]:///set`                    |
| `NANTS=[num antennas]`            | Number of antennas in the subarray.                                                              | `[HPGDOMAIN]:///set`                    |
| `CHAN_BW=[channel bandwidth]`     | Bandwidth of a single F-engine frequency channel (coarse channel).                               | `[HPGDOMAIN]:///set`                    |
| `PKTSTART=[starting packet index]`| Index of the first packet from which to record (to ensure a synchronised start across instances).| `[HPGDOMAIN]:///set`                    |
| `FESTATUS=[data-suspect bitmask]` | Hex mask indicating data suspect for each polarisation from each F-eng. 1 = data is suspect.  | `[HPGDOMAIN]:///set`                    |
| `FECENTER=[centre frequency]`     | On-sky observation centre frequency.                                                             | `[HPGDOMAIN]:///set`                    |
| `DWELL=[dwell time]`              | Duration of recording. Can also be used to halt a recording.                                     | `[HPGDOMAIN]:///set`                    |
| `SRC_NAME=[source name]`          | Source name (description retrieved from CAM).                                                    | `[HPGDOMAIN]:///set`                    |
| `AZ=[azimuth]`                    | Azimuth (in degrees).                                                                            | `[HPGDOMAIN]:///set`                    |
| `EL=[elevation]`                  | Elevation (in degrees).                                                                          | `[HPGDOMAIN]:///set`                    |
| `RA_STR=[RA]`                     | RA (string form).                                                                                | `[HPGDOMAIN]:///set`                    |
| `DEC_STR=[Dec]`                   | Declination (string form).                                                                       | `[HPGDOMAIN]:///set`                    |
| `RA=[RA]`                         | RA (degrees).                                                                                    | `[HPGDOMAIN]:///set`                    |
| `DEC=[Dec]`                       | Declination (degrees).                                                                           | `[HPGDOMAIN]:///set`                    |
