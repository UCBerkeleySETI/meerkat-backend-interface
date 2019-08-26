# meerkat-backend-interface

---

This repository is for the `KATCP Server`, `Katportal Client` and `Coordinator`  modules of the diagram below. Together, these modules extract all observational metadata to enable Breakthrough Listen's automated commensal observing program on MeerKAT. These data include:
* Addresses of SPEAD packet streams containing raw channelised antenna voltage data, to be subscribed to by our processing nodes for beamforming.
* Current target information
* Schedule block information
* Antenna weights
* Comprehensive sensor data

###  Diagram of Breakthrough Listen's Commensal Observing System at MeerKAT

![diagram](diagram.png)

### CAM
The **C**ontrol **A**nd **M**onitoring system for the telescope.

### KATCP Server
The `KATCP Server` receives requests from `CAM`. These requests include:

* ?configure
* ?capture-init
* ?capture-start
* ?capture-stop
* ?capture-done
* ?deconfigure
* ?halt
* ?help
* ?log-level
* ?restart [#restartf1]_
* ?client-list
* ?sensor-list
* ?sensor-sampling
* ?sensor-value
* ?watchdog
* ?version-list (only standard in KATCP v5 or later)
* ?request-timeout-hint (pre-standard only if protocol flags indicates timeout hints, supported for KATCP v5.1 or later)
* ?sensor-sampling-clear (non-standard)

The `?configure` `?capture-init` `?capture-start` `?capture-stop` `?capture-done` `?deconfigure` and `?halt` requests have custom implementations in `katcp_server.py`'s `BLBackendInterface` class. The rest are inherited from its superclass. Together, these requests (particularly `?configure`) contain important metadata, such as the URLs for the raw voltage data coming off the telescope, and their timing is important too. For instance, we'll know when an observation has started when we receive the `?capture-start` request. For more information, see the [ICD](https://docs.google.com/document/d/19GAZYT5OI1CLqoWU8Q2urBUYyTVfvWktsH4yTegrF_0/edit) and the [Swim Lane Diagram](https://docs.google.com/spreadsheets/d/1U9Un2jd3GsgTeaJ96GhQPXckZkG_TdRd0DCsaxFeX3Q/edit#gid=0)

### Redis Server
A redis database is a key-value store that is useful for sharing information between modules in a system like this one. It runs as a server on the local network that can be modified and accessed via requests. It has a command-line client, but the [redis-py](https://github.com/andymccurdy/redis-py) python module is used here.

### Katportal Client
The `Katportal Client` sends requests for additional metadata to `CAM`. 

### Target Selection & Beam Forming
System that selects targets from and forms pencil-beams to observe them. The target list has been compiled by Logan Pierce and the target selector component has been written by Tyler Cox. 

### Real-time Signal Detection and Data Storage
Our signal detection and data storage systems.

## Installation Instructions

There may be other ways of doing it, but this way works for me as of 2019-03-31. First, make sure to [install redis](https://redis.io/topics/quickstart). If it's installed, you should be able to start it by executing from the terminal:
```
redis-server
```
Next, download the repository:
```
git clone --recurse-submodules https://github.com/ejmichaud/meerkat-backend-interface
```
It's important to include the `--recurse-submodules` because certain components rely on what's in the `./reynard/` submodule that is installed with this. 

**Now, install the following Python packages in precisely the order listed**. Of course, I'd recommend installing everything in a Python 2 virtual environment (create one with `virtualenv -p <python2 binary> venv` and then activate with `. venv/bin/activate`)

First, make sure you `cd meerkat-backend-interface`, then:

1. `pip install katversion`
2. `pip install -r requirements.txt`

You should hopefully then be able to run all the modules. 

## Usage
After starting redis, the processes can be started manually as given below. However, the intention is that they be run using `circusd` or similar.
```
(venv)$ python katcp_start.py --ip <fixed localhost ip address> --port <port> --nodeset <nodeset>
```
```
(venv)$ python katportal_start.py --config <config file>
```
```
(venv)$ python coordinator.py --port <redis port> --config <config file>
```

## Redis Formatting:
For redis key formatting and respective value descriptions, see [REDIS_DOCUMENTATION](docs/REDIS_DOCUMENTATION.md)

### Current limitations/considerations:
* Currently, sensor values are stored in redis associated with the `product_id` of the subarray which they were queried from (see [REDIS_DOCUMENTATION](docs/REDIS_DOCUMENTATION.md)). Since product ids are temporary, this could lead to a build-up of outdated sensor values in the redis server, since a new key is created for each product_id * for each sensor. Some possibilities might be to set expiration times for keys, or to explicitly delete them when no longer needed. 
* Currently, `katportal_start.py` does not shut down in a thread-safe way. `katcp_start.py` manages to do this, but it uses a complex mechanism that I don't understand. Consider supporting thread-safe shutdown of `src/katportal_server.py`'s io_loop in the future.
* While currently commented out, there is a mechanism that sends a slack message when either of the modules shuts down. For testing purposes, this has been commented out. When you deploy, you should uncomment it, and possibly modify its behavior. As you can see in the `src/slack_tools.py` file, using this requires you to store a Unix variable called `$SLACK_TOKEN` in your local environment. 

**Tasks are marked in the code with a `TODO` keyword.

## Questions:

1. In [1], Siemion et al. say that the "SETI Search Subsystem" will detect candidate signals in **real time**. What needs to happen from a software perspective for this to happen? Will those processes be started and monitored from this system, or from something lower in the stack?
2. A **significant task** seems to be the design of the software-based beamforming/interferometry systems. In [1], Siemion et al. say that our system will start with a single beam and then upgrade from there. Do we still plan on scaling this way? If so, do we intend on writing the software now that will support the multi-beam observing when it comes online? Will the system that we are building now be sufficiently general to handle all sub-array sizes and beam configurations going forward?
3. In [1], Siemion et al. write that "A small amount of observing time or follow up observations, 5-10 hours per month, would significantly improve the speed and productivity of the commensal program." What kind of human interface do we need to build in order to accomodate these observations? Will these targets be selected manually (in contrast to the automated "scheduling" system that will do most of our commensal observing)?
4. Will this same software be used on the SKA? (approx. 2022?)

## References

1. [SETI on MeerKAT Project Proposal, Siemion et al.](https://www.overleaf.com/5968578fnxfyc#/19819904/)
2. [katcp-python GitHub repository](https://github.com/ska-sa/katcp-python)
3. [katportalclient GitHub repository](https://github.com/ska-sa/katportalclient)
4. [MeerKAT ICD](https://docs.google.com/document/d/19GAZYT5OI1CLqoWU8Q2urBUYyTVfvWktsH4yTegrF_0/edit#)
5. [TRAPUM-CAM Interface](https://github.com/ewanbarr/reynard/tree/refactor)
6. [Swim Lane Diagram](https://docs.google.com/spreadsheets/d/1U9Un2jd3GsgTeaJ96GhQPXckZkG_TdRd0DCsaxFeX3Q/edit#gid=0)
7. [Katportal Docs](https://docs.google.com/document/d/1BD22ZwaVwHiB6vxc0ryP9vUXnFAsTbmD8K2oBPRPWCo/edit). 
8. [REDIS_DOCUMENTATION](docs/REDIS_DOCUMENTATION.md)
