/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SubtaskStateStatsTest {

    /** Tests simple access via the getters. */
    @Test
    void testSimpleAccess() throws Exception {
        test(false);
    }

    /** Tests that the snapshot is actually serializable. */
    @Test
    void testIsJavaSerializable() throws Exception {
        test(true);
    }

    public void test(boolean serialize) throws Exception {
        SubtaskStateStats stats =
                new SubtaskStateStats(
                        0,
                        Integer.MAX_VALUE + 1L,
                        Integer.MAX_VALUE + 2L,
                        Integer.MAX_VALUE + 2L,
                        Integer.MAX_VALUE + 3L,
                        Integer.MAX_VALUE + 4L,
                        Integer.MAX_VALUE + 8L,
                        Integer.MAX_VALUE + 9L,
                        Integer.MAX_VALUE + 6L,
                        Integer.MAX_VALUE + 7L,
                        false,
                        true);

        stats = serialize ? CommonTestUtils.createCopySerializable(stats) : stats;

        assertThat(stats.getSubtaskIndex()).isZero();
        assertThat(stats.getAckTimestamp()).isEqualTo(Integer.MAX_VALUE + 1L);
        assertThat(stats.getStateSize()).isEqualTo(Integer.MAX_VALUE + 2L);
        assertThat(stats.getSyncCheckpointDuration()).isEqualTo(Integer.MAX_VALUE + 3L);
        assertThat(stats.getAsyncCheckpointDuration()).isEqualTo(Integer.MAX_VALUE + 4L);
        assertThat(stats.getAlignmentDuration()).isEqualTo(Integer.MAX_VALUE + 6L);
        assertThat(stats.getCheckpointStartDelay()).isEqualTo(Integer.MAX_VALUE + 7L);

        // Check duration helper
        long ackTimestamp = stats.getAckTimestamp();
        long triggerTimestamp = ackTimestamp - 10123;
        assertThat(stats.getEndToEndDuration(triggerTimestamp)).isEqualTo(10123);

        // Trigger timestamp < ack timestamp
        assertThat(stats.getEndToEndDuration(ackTimestamp + 1)).isZero();
    }
}
