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

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link StreamPartitioner#isSupportsUnalignedCheckpoint(boolean)} — specifically the
 * {@code forceUnaligned} flag introduced to allow unaligned checkpoints on forward (pointwise)
 * edges while keeping broadcast edges aligned.
 */
class ForceUnalignedSupportTest {

    @Test
    void testForwardPartitionerRespectsForceUnaligned() {
        ForwardPartitioner<Tuple> partitioner = new ForwardPartitioner<>();

        // Without force: pointwise edges block unaligned checkpoints.
        assertThat(partitioner.isSupportsUnalignedCheckpoint(false)).isFalse();
        // With force: pointwise edges are allowed to use unaligned checkpoints.
        assertThat(partitioner.isSupportsUnalignedCheckpoint(true)).isTrue();
    }

    @Test
    void testRescalePartitionerRespectsForceUnaligned() {
        RescalePartitioner<Tuple> partitioner = new RescalePartitioner<>();

        assertThat(partitioner.isSupportsUnalignedCheckpoint(false)).isFalse();
        assertThat(partitioner.isSupportsUnalignedCheckpoint(true)).isTrue();
    }

    @Test
    void testBroadcastPartitionerIgnoresForceUnaligned() {
        BroadcastPartitioner<Tuple> partitioner = new BroadcastPartitioner<>();

        // Broadcast edges must never support unaligned checkpoints, regardless of force.
        assertThat(partitioner.isSupportsUnalignedCheckpoint(false)).isFalse();
        assertThat(partitioner.isSupportsUnalignedCheckpoint(true)).isFalse();
    }

    @Test
    void testRebalancePartitionerForceDoesNotFlipBehavior() {
        RebalancePartitioner<Tuple> partitioner = new RebalancePartitioner<>();

        boolean withoutForce = partitioner.isSupportsUnalignedCheckpoint(false);
        boolean withForce = partitioner.isSupportsUnalignedCheckpoint(true);

        assertThat(withoutForce).isTrue();
        assertThat(withForce).isEqualTo(withoutForce);
    }

    @Test
    void testKeyGroupPartitionerForceDoesNotFlipBehavior() {
        KeyGroupStreamPartitioner<Tuple, Object> partitioner =
                new KeyGroupStreamPartitioner<>((KeySelector<Tuple, Object>) value -> value, 128);

        boolean withoutForce = partitioner.isSupportsUnalignedCheckpoint(false);
        boolean withForce = partitioner.isSupportsUnalignedCheckpoint(true);

        assertThat(withoutForce).isTrue();
        assertThat(withForce).isEqualTo(withoutForce);
    }
}
