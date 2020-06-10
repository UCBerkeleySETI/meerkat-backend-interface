# KATCP Server

The `KATCP Server` receives requests from `CAM`. These requests may include:

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

The `?configure` `?capture-init` `?capture-start` `?capture-stop` `?capture-done` `?deconfigure` and `?halt` requests have custom implementations in `katcp_server.py`'s `BLBackendInterface` class. The rest are inherited from its superclass. Together, these requests (particularly `?configure`) contain important metadata, such as the URLs for the raw voltage data streams from the F-engines. The timing of these requests is important too. For instance, the `capture-start` request is sent when an observation is about to begin. 

For more information, please see: 
* The [ICD](https://docs.google.com/document/d/19GAZYT5OI1CLqoWU8Q2urBUYyTVfvWktsH4yTegrF_0/edit) 
* The [Swim Lane Diagram](https://docs.google.com/spreadsheets/d/1U9Un2jd3GsgTeaJ96GhQPXckZkG_TdRd0DCsaxFeX3Q/edit#gid=0)