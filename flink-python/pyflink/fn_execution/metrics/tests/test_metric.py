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
from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.execution import MetricsContainer
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.metrics.metricbase import MetricName
from apache_beam.runners.worker import statesampler
from apache_beam.utils import counters

from pyflink.fn_execution.metrics.process.metric_impl import GenericMetricGroup
from pyflink.metrics.metricbase import MetricGroup
from pyflink.table import FunctionContext
from pyflink.testing.test_case_utils import PyFlinkTestCase


class MetricTests(PyFlinkTestCase):

    base_metric_group = GenericMetricGroup(None, None)

    @staticmethod
    def print_metric_group_path(mg: MetricGroup) -> str:
        if mg._parent is None:
            return 'root'
        else:
            return MetricTests.print_metric_group_path(mg._parent) + '.' + mg._name

    def test_add_group(self):
        new_group = MetricTests.base_metric_group.add_group('my_group')
        self.assertEqual(MetricTests.print_metric_group_path(new_group), 'root.my_group')

    def test_add_group_with_variable(self):
        new_group = MetricTests.base_metric_group.add_group('key', 'value')
        self.assertEqual(MetricTests.print_metric_group_path(new_group), 'root.key.value')

    def test_metric_not_enabled(self):
        fc = FunctionContext(None, None)
        with self.assertRaises(RuntimeError):
            fc.get_metric_group()

    def test_function_context_runtime_info(self):
        fc = FunctionContext(
            None, {},
            task_name='MyTask',
            task_name_with_subtasks='MyTask (1/4)',
            number_of_parallel_subtasks=4,
            max_number_of_parallel_subtasks=128,
            index_of_this_subtask=0,
            attempt_number=2)
        self.assertEqual('MyTask', fc.get_task_name())
        self.assertEqual('MyTask (1/4)', fc.get_task_name_with_subtasks())
        self.assertEqual(4, fc.get_number_of_parallel_subtasks())
        self.assertEqual(128, fc.get_max_number_of_parallel_subtasks())
        self.assertEqual(0, fc.get_index_of_this_subtask())
        self.assertEqual(2, fc.get_attempt_number())

    def test_function_context_runtime_info_defaults(self):
        fc = FunctionContext(None, {})
        self.assertIsNone(fc.get_task_name())
        self.assertIsNone(fc.get_task_name_with_subtasks())
        self.assertIsNone(fc.get_number_of_parallel_subtasks())
        self.assertIsNone(fc.get_max_number_of_parallel_subtasks())
        self.assertIsNone(fc.get_index_of_this_subtask())
        self.assertIsNone(fc.get_attempt_number())

    def test_get_metric_name(self):
        new_group = MetricTests.base_metric_group.add_group('my_group')
        self.assertEqual(
            '["my_group", "MetricGroupType.generic"]',
            new_group._get_namespace())
        self.assertEqual(
            '["my_group", "MetricGroupType.generic", "60"]',
            new_group._get_namespace('60'))

    def test_metrics(self):
        sampler = statesampler.StateSampler('', counters.CounterFactory())
        statesampler.set_current_tracker(sampler)
        state1 = sampler.scoped_state(
            'mystep', 'myState', metrics_container=MetricsContainer('mystep'))

        try:
            sampler.start()
            with state1:
                counter = MetricTests.base_metric_group.counter("my_counter")
                meter = MetricTests.base_metric_group.meter("my_meter")
                distribution = MetricTests.base_metric_group.distribution("my_distribution")
                container = MetricsEnvironment.current_container()

                self.assertEqual(0, counter.get_count())
                self.assertEqual(0, meter.get_count())
                self.assertEqual(
                    DistributionData(
                        0, 0, 0, 0), container.get_distribution(
                        MetricName(
                            '[]', 'my_distribution')).get_cumulative())
                counter.inc(-2)
                meter.mark_event(3)
                distribution.update(10)
                distribution.update(2)
                self.assertEqual(-2, counter.get_count())
                self.assertEqual(3, meter.get_count())
                self.assertEqual(
                    DistributionData(
                        12, 2, 2, 10), container.get_distribution(
                        MetricName(
                            '[]', 'my_distribution')).get_cumulative())
        finally:
            sampler.stop()
