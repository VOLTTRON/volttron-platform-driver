# Platform Driver Agent

![Passing?](https://github.com/eclipse-volttron/volttron-platform-driver/actions/workflows/run-tests.yml/badge.svg)
[![pypi version](https://img.shields.io/pypi/v/volttron-platform-driver.svg)](https://pypi.org/project/volttron-platform-driver/)


The Platform Driver agent is a special purpose agent a user can install on the platform to manage communication of the platform with devices. The Platform driver features a number of endpoints for collecting data and sending control signals using the message bus and automatically publishes data to the bus on a specified interval.

# Requires

* python >= 3.10
* volttron.core >= 2.0.0rc0
* volttron-lib-base-driver >= 2.0.0rc0

# Documentation
More detailed documentation can be found on [ReadTheDocs](https://eclipse-volttron.readthedocs.io/en/latest/external-docs/volttron-platform-driver/index.html). The RST source
of the documentation for this component is located in the "docs" directory of this repository.
This documentation is current up to version 1.x of the Platform Driver Agent and will be updated
to reflect new features and behaviors in the course of RC releases. Most existing configurations
and behaviors remain valid, however, so this remains a good source of information.

#### New Polling Features
The Platform Driver version 2.0 does introduce multiple new capabilities.
A detailed table showing current state of completion of features to be included in
the full 2.0.0 release can be found [here](driver_status_2.0.0rc0.png).
In addition to an expanded API, as summarized below, the new driver contains several
features intended to make polling more scalable, flexible, and efficient:

* Poll rates may now be set on individual points by adding a 'polling_interval' colunn
  to the registry.  Any point which does not have a polling interval set will fall back
  to the default interval defined in the device configuration.
* In the case where multiple devices within VOLTTRON correspond to a single remote on the
  network (that is the driver_configs dictionaries are the same), these will by default be
  polled in a single request, when possible, to the remote. No additional configuration
  is required to make use of this feature, but it can be disabled, if desired, by
  setting the "allow_duplicate_remotes" setting to True in either the agent configuration
  (as the default for all devices) or separately in the configuration for each device which
  should not share its remote with other devices.
* Last known values and times at which these were obtained are stored
  in a data model and can be retrieved with the "last" RPC method.

#### Publication Changes
As the direct result of the new capability for points to have different polling rates,
all points are no longer guaranteed to be obtained on every poll. In the previous driver
implementation, an all publish would be sent at the completion of each poll with the data
obtained from all configured points on the device. In the new design, however, only a subset
of points is guaranteed to be new. For this reason, the default behavior is now to publish
on a topic ending in "/multi". The multi-style publish is formatted identically to the all-style
publishes, except that it does not necessarily always contain every point on the device.

For applications which require all points to be available in a single publish, an all-style
publish may still be configured.  This can be done by setting the "publish_all_depth" key to True
in the device configuration file for any devices which should be published in this manner.
(The key worded "publish_depth_first_all" will continue to work as well for the same purpose.)
Additionally, an "all_publish_interval" should be provided as a number of seconds between publishes.
These settings may also be set in the agent configuration if the same behavior is desired for all
configured devices.

All-type publishes will begin once a first round of polling has completed for all points, and will contain
the last known value for each point at the time of the poll. A "stale_timeout" setting may
be configured for the entire device or in the registry on a point-by-point basis. All publishes
will only occur if all points have not become stale. The default state_timeout is 3 times the
length of the polling interval for any point.

#### Expanded API
The RPC API for the driver has been expanded both for the addition of new features and also as the result
of merging the functionality of the former Actuator Agent into the Platform Driver Agent itself.

The following new methods are provided for more flexible queries.

```
get(topic: str | Sequence | Set = None, regex: str = None) -> (dict, dict)

set(value: any, topic: str | Sequence | Set = None, regex: str = None) -> (dict, dict)

revert(topic: str | Sequence | Set = None, regex: str = None) -> (dict, dict)

last(topic: str | Sequence | Set = None, regex: str = None, value: bool = True, updated:bool = True) -> dict
```

Astute observers will notice that these methods largely share common arguments,
which are described below. Set also takes a value or mapping of values which will be
written to points. Last can also be configured to return values, update times, or both
(using the value and updated boolean arguments).

They each return two dicts --- the first for results and the second for errors.
The last method, which returns last known values and/or updated times for a set of points,
does not need to make a network request to obtain its data, it provides only a results dictionary.


* **topic**: This can be one or more topics. 
  * Where the topic describes more than a -single point, all points corresponding to the topic will be returned.
  * The '-' character may also be used as a wildcard to replace any complete segment in a topic string. For instance:
    `devices/Campus/Building1/-/ZoneTemperatureSetPoint` would match all devices in Building1
    with a point called "ZoneTemperatureSetPoint".
* **regex**: The set of points obtained from the topic argument may be further refined using
  a regular expression. If no topic is provided at all, the regular expression will be applied
  to all topics known to the driver.


All existing RPC methods from both the Platform Driver and Actuator agents continue to work as before.
In cases where both agents had methods with the same name, effort has been made to preserve the ability to
use either style of arguments to these functions. The one corner case which is known to not work is if
the caller passed a string to the actuator agent for the requester_id when setting points.
This will only work if the string passed was the vip-identity of the agent. If this argument is left out, however,
or the vip-identity is used, then it should continue to work as expected.


# Installation

Before installing, VOLTTRON should be installed and running.  Its virtual environment should be active.
Information on how to install of the VOLTTRON platform can be found
[here](https://github.com/eclipse-volttron/volttron-core).

Install and start the volttron-platform-driver.

```shell
vctl install volttron-platform-driver --vip-identity platform.driver
```

View the status of the installed agent

```shell
vctl status
```

To communicate with devices, one or more driver interfaces will also need to be installed.
Each interface is distributed as a library and may be installed separately using pip.
In the current RC version of the driver, only two interfaces are fully supported:

* A Fake Driver (which returns data from a csv file):
    ```shell
    pip install volttron-lib-fake-driver
    ```
* BACnet:
    ```shell
    pip install volttron-lib-fake-driver
    ```

Additional interfaces will be available in later RC releases.

# Configuration

Existing configuration files should generally continue to work as expected.


# Development

Please see the following for contributing guidelines [contributing](https://github.com/eclipse-volttron/volttron-core/blob/develop/CONTRIBUTING.md).

Please see the following helpful guide about [developing modular VOLTTRON agents](https://github.com/eclipse-volttron/volttron-core/blob/develop/DEVELOPING_ON_MODULAR.md)


# Disclaimer Notice

This material was prepared as an account of work sponsored by an agency of the
United States Government.  Neither the United States Government nor the United
States Department of Energy, nor Battelle, nor any of their employees, nor any
jurisdiction or organization that has cooperated in the development of these
materials, makes any warranty, express or implied, or assumes any legal
liability or responsibility for the accuracy, completeness, or usefulness or any
information, apparatus, product, software, or process disclosed, or represents
that its use would not infringe privately owned rights.

Reference herein to any specific commercial product, process, or service by
trade name, trademark, manufacturer, or otherwise does not necessarily
constitute or imply its endorsement, recommendation, or favoring by the United
States Government or any agency thereof, or Battelle Memorial Institute. The
views and opinions of authors expressed herein do not necessarily state or
reflect those of the United States Government or any agency thereof.
