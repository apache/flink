/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.streamtask;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamTaskTestHarness;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StreamIterationHead}. */
class StreamIterationHeadTest {

    @Test
    void testIterationHeadWatermarkEmission() throws Exception {
        StreamTaskTestHarness<Integer> harness =
                new StreamTaskTestHarness<>(StreamIterationHead::new, BasicTypeInfo.INT_TYPE_INFO);
        harness.setupOutputForSingletonOperatorChain();
        harness.getStreamConfig().setIterationId("1");
        harness.getStreamConfig().setIterationWaitTime(1);

        harness.invoke();
        harness.waitForTaskCompletion();

        assertThat(harness.getOutput()).hasSize(1);
        assertThat(harness.getOutput().peek()).isEqualTo(new Watermark(Long.MAX_VALUE));
    }
}
