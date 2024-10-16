# -*- coding: utf-8 -*- {{{
# ===----------------------------------------------------------------------===
#
#                 Installable Component of Eclipse VOLTTRON
#
# ===----------------------------------------------------------------------===
#
# Copyright 2022 Battelle Memorial Institute
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# ===----------------------------------------------------------------------===
# }}}

from volttron.client.messaging import topics as t

VALUE_RESPONSE_PREFIX = t.ACTUATOR_VALUE()
REVERT_POINT_RESPONSE_PREFIX = t.ACTUATOR_REVERTED_POINT()
REVERT_DEVICE_RESPONSE_PREFIX = t.ACTUATOR_REVERTED_DEVICE()
ERROR_RESPONSE_PREFIX = t.ACTUATOR_ERROR()

WRITE_ATTEMPT_PREFIX = t.ACTUATOR_WRITE()

GET_TOPIC = t.ACTUATOR_GET()
RESERVATION_REQUEST_TOPIC = t.ACTUATOR_SCHEDULE_REQUEST()
RESERVATION_RESULT_TOPIC = t.ACTUATOR_SCHEDULE_RESULT()
REVERT_DEVICE_TOPIC = t.ACTUATOR_REVERT_DEVICE()
REVERT_POINT_TOPIC = t.ACTUATOR_REVERT_POINT()
SET_TOPIC = t.ACTUATOR_SET()

RESERVATION_ACTION_NEW = 'NEW_RESERVATION'
RESERVATION_ACTION_CANCEL = 'CANCEL_RESERVATION'
LEGACY_RESERVATION_ACTION_NEW = 'NEW_SCHEDULE'
LEGACY_RESERVATION_ACTION_CANCEL = 'CANCEL_SCHEDULE'

RESERVATION_RESPONSE_SUCCESS = 'SUCCESS'
RESERVATION_RESPONSE_FAILURE = 'FAILURE'

RESERVATION_CANCEL_PREEMPTED = 'PREEMPTED'

ACTUATOR_COLLECTION = 'actuators'