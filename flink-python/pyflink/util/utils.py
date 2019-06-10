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
import sys
from datetime import timedelta

from pyflink.java_gateway import get_gateway

if sys.version >= '3':
    unicode = str


def to_jarray(j_type, arr):
    """
    Convert python list to java type array

    :param j_type: java type of element in array
    :param arr: python type list
    """
    gateway = get_gateway()
    j_arr = gateway.new_array(j_type, len(arr))
    for i in range(0, len(arr)):
        j_arr[i] = arr[i]
    return j_arr


def to_j_flink_time(time_delta):
    gateway = get_gateway()
    TimeUnit = gateway.jvm.java.util.concurrent.TimeUnit
    Time = gateway.jvm.org.apache.flink.api.common.time.Time
    if isinstance(time_delta, timedelta):
        total_microseconds = round(time_delta.total_seconds() * 1000 * 1000)
        return Time.of(total_microseconds, TimeUnit.MICROSECONDS)
    else:
        # time delta in milliseconds
        total_milliseconds = time_delta
        return Time.milliseconds(total_milliseconds)


def from_j_flink_time(j_flink_time):
    total_milliseconds = j_flink_time.toMilliseconds()
    return timedelta(milliseconds=total_milliseconds)


def load_java_class(class_name):
    gateway = get_gateway()
    context_classloader = gateway.jvm.Thread.currentThread().getContextClassLoader()
    return context_classloader.loadClass(class_name)
