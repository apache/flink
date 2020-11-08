################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from pyflink.java_gateway import get_gateway


class Duration(object):
    """
    A time-based amount of time, such as '34.5 seconds'.
    """
    def __init__(self, j_duration):
        self._j_duration = j_duration

    @staticmethod
    def of_days(days: int):
        return Duration(get_gateway().jvm.java.time.Duration.ofDays(days))

    @staticmethod
    def of_hours(hours: int):
        return Duration(get_gateway().jvm.java.time.Duration.ofHours(hours))

    @staticmethod
    def of_millis(millis: int):
        return Duration(get_gateway().jvm.java.time.Duration.ofMillis(millis))

    @staticmethod
    def of_minutes(minutes: int):
        return Duration(get_gateway().jvm.java.time.Duration.ofMinutes(minutes))

    @staticmethod
    def of_nanos(nanos: int):
        return Duration(get_gateway().jvm.java.time.Duration.ofNanos(nanos))

    @staticmethod
    def of_seconds(seconds: int):
        return Duration(get_gateway().jvm.java.time.Duration.ofSeconds(seconds))
