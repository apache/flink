.. ################################################################################
     Licensed to the Apache Software Foundation (ASF) under one
     or more contributor license agreements.  See the NOTICE file
     distributed with this work for additional information
     regarding copyright ownership.  The ASF licenses this file
     to you under the Apache License, Version 2.0 (the
     "License"); you may not use this file except in compliance
     with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
    limitations under the License.
   ################################################################################


=====
Timer
=====

TimerService
------------

.. currentmodule:: pyflink.datastream.timerservice

.. autosummary::
    :toctree: api/

    TimerService.current_processing_time
    TimerService.current_watermark
    TimerService.register_processing_time_timer
    TimerService.register_event_time_timer
    TimerService.delete_processing_time_timer
    TimerService.delete_event_time_timer


TimeCharacteristic
------------------

.. currentmodule:: pyflink.datastream.time_characteristic

.. autosummary::
    :toctree: api/

    TimeCharacteristic


TimeDomain
----------

.. currentmodule:: pyflink.datastream.time_domain

.. autosummary::
    :toctree: api/

    TimeDomain
