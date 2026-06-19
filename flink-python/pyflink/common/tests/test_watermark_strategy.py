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
from pyflink.common import Duration
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import is_instance_of, get_field_value

from pyflink.testing.test_case_utils import PyFlinkTestCase


class WatermarkStrategyTests(PyFlinkTestCase):

    def test_with_idleness(self):
        jvm = get_gateway().jvm
        j_watermark_strategy = WatermarkStrategy.no_watermarks().with_idleness(
            Duration.of_seconds(5)
        )._j_watermark_strategy
        self.assertTrue(is_instance_of(
            j_watermark_strategy,
            jvm.org.apache.flink.api.common.eventtime.WatermarkStrategyWithIdleness
        ))
        self.assertEqual(get_field_value(j_watermark_strategy, "idlenessTimeout").toMillis(), 5000)

    def test_with_watermark_alignment(self):
        jvm = get_gateway().jvm
        j_watermark_strategy = WatermarkStrategy.no_watermarks().with_watermark_alignment(
            "alignment-group-1", Duration.of_seconds(20), Duration.of_seconds(10)
        )._j_watermark_strategy
        self.assertTrue(is_instance_of(
            j_watermark_strategy,
            jvm.org.apache.flink.api.common.eventtime.WatermarksWithWatermarkAlignment
        ))
        alignment_parameters = j_watermark_strategy.getAlignmentParameters()
        self.assertEqual(alignment_parameters.getWatermarkGroup(), "alignment-group-1")
        self.assertEqual(alignment_parameters.getMaxAllowedWatermarkDrift(), 20000)
        self.assertEqual(alignment_parameters.getUpdateInterval(), 10000)

    def test_for_monotonous_timestamps(self):
        jvm = get_gateway().jvm
        j_watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()._j_watermark_strategy
        self.assertTrue(is_instance_of(
            j_watermark_strategy.createWatermarkGenerator(None),
            jvm.org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks
        ))

    def test_for_bounded_out_of_orderness(self):
        jvm = get_gateway().jvm
        j_watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(3)
        )._j_watermark_strategy
        j_watermark_generator = j_watermark_strategy.createWatermarkGenerator(None)
        self.assertTrue(is_instance_of(
            j_watermark_generator,
            jvm.org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks
        ))
        self.assertEqual(get_field_value(j_watermark_generator, "outOfOrdernessMillis"), 3000)

    def test_no_watermarks(self):
        jvm = get_gateway().jvm
        j_watermark_strategy = WatermarkStrategy.no_watermarks()._j_watermark_strategy
        self.assertTrue(is_instance_of(
            j_watermark_strategy.createWatermarkGenerator(None),
            jvm.org.apache.flink.api.common.eventtime.NoWatermarksGenerator
        ))
