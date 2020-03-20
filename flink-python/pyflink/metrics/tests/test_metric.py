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

import unittest
from pyflink.metrics.metricbase import GenericMetricGroup


class MetricTests(unittest.TestCase):

    base_metric_group = GenericMetricGroup(None, 'base', {'key': 'value'}, ['key', 'value'], '.')

    def test_add_group(self):
        new_group = MetricTests.base_metric_group.add_group('my_group')
        self.assertEqual(new_group.get_all_variables(), {'key': 'value'})
        self.assertEqual(new_group.get_scope_components(), ['key', 'value', 'my_group'])
        self.assertEqual(new_group.get_metric_identifier(
            'my_metric'), 'key.value.my_group.my_metric')

    def test_add_group_with_variable(self):
        new_group = MetricTests.base_metric_group.add_group('my_key_group', 'my_value_group')
        self.assertEqual(
            new_group.get_all_variables(), {
                'key': 'value', 'my_key_group': 'my_value_group'})
        self.assertEqual(
            new_group.get_scope_components(), [
                'key', 'value', 'my_key_group', 'my_value_group'])
        self.assertEqual(new_group.get_metric_identifier('my_metric'),
                         'key.value.my_key_group.my_value_group.my_metric')

    def test_variable_replace(self):
        self.assertEqual(MetricTests.base_metric_group.get_all_variables(), {'key': 'value'})
        self.assertEqual(MetricTests.base_metric_group.get_scope_components(), ['key', 'value'])
        self.assertEqual(
            MetricTests.base_metric_group.get_metric_identifier('my_metric'),
            'key.value.my_metric')

        # test variable replace
        new_group = MetricTests.base_metric_group.add_group('key', 'new_value')
        self.assertEqual(new_group.get_all_variables(), {'key': 'new_value'})
        self.assertEqual(new_group.get_scope_components(), ['key', 'value', 'key', 'new_value'])
        self.assertEqual(
            new_group.get_metric_identifier('my_metric'),
            'key.value.key.new_value.my_metric')
