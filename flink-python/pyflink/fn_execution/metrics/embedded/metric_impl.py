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
from typing import Callable

from pemja import findClass

from pyflink.fn_execution.metrics.embedded.counter_impl import CounterImpl
from pyflink.fn_execution.metrics.embedded.distribution_impl import DistributionImpl
from pyflink.fn_execution.metrics.embedded.meter_impl import MeterImpl
from pyflink.metrics import MetricGroup, Counter, Distribution, Meter

JMeterView = findClass('org.apache.flink.metrics.MeterView')
JMetricGauge = findClass('org.apache.flink.python.metric.embedded.MetricGauge')
JMetricDistribution = findClass('org.apache.flink.python.metric.embedded.MetricDistribution')


class MetricGroupImpl(MetricGroup):

    def __init__(self, metrics):
        self._metrics = metrics

    def add_group(self, name: str, extra: str = None) -> 'MetricGroup':
        if extra is None:
            return MetricGroupImpl(self._metrics.addGroup(name))
        else:
            return MetricGroupImpl(self._metrics.addGroup(name, extra))

    def counter(self, name: str) -> 'Counter':
        return CounterImpl(self._metrics.counter(name))

    def gauge(self, name: str, obj: Callable[[], int]) -> None:
        self._metrics.gauge(name, JMetricGauge(PythonGaugeCallable(obj)))

    def meter(self, name: str, time_span_in_seconds: int = 60) -> 'Meter':
        return MeterImpl(self._metrics.meter(name, JMeterView(time_span_in_seconds)))

    def distribution(self, name: str) -> 'Distribution':
        return DistributionImpl(self._metrics.gauge(name, JMetricDistribution()))


class PythonGaugeCallable(object):
    def __init__(self, func: Callable):
        self.func = func

    def get_value(self):
        return self.func()
