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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.JobManagerOptions.HybridConsumePartitionMode;
import org.apache.flink.runtime.scheduler.strategy.AllFinishedInputConsumableDecider;
import org.apache.flink.runtime.scheduler.strategy.DefaultInputConsumableDecider;
import org.apache.flink.runtime.scheduler.strategy.InputConsumableDecider;
import org.apache.flink.runtime.scheduler.strategy.PartialFinishedInputConsumableDecider;

import org.junit.jupiter.api.Test;

import static org.apache.flink.configuration.JobManagerOptions.HybridConsumePartitionMode.CAN_CONSUME_PARTIAL_FINISHED;
import static org.apache.flink.configuration.JobManagerOptions.HybridConsumePartitionMode.CAN_CONSUME_UN_FINISHED;
import static org.apache.flink.configuration.JobManagerOptions.HybridConsumePartitionMode.ONLY_CONSUME_ALL_FINISHED;
import static org.apache.flink.runtime.scheduler.adaptivebatch.AdaptiveBatchSchedulerFactory.getOrDecideHybridConsumePartitionMode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AdaptiveBatchSchedulerFactory}. */
class AdaptiveBatchSchedulerFactoryTest {
    @Test
    void testNotOnlyConsumeFinishedPartitionWithSpeculativeEnable() {
        Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.HYBRID_CONSUME_PARTITION_MODE, CAN_CONSUME_UN_FINISHED);
        assertThatThrownBy(() -> getOrDecideHybridConsumePartitionMode(configuration, true))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testOnlyConsumeFinishedPartitionWillSetForSpeculativeEnable() {
        HybridConsumePartitionMode hybridConsumePartitionMode =
                getOrDecideHybridConsumePartitionMode(new Configuration(), true);
        assertThat(hybridConsumePartitionMode.isOnlyConsumeFinishedPartition()).isTrue();
    }

    @Test
    void testLoadInputConsumableDeciderFactory() {
        assertAndLoadInputConsumableDecider(
                CAN_CONSUME_UN_FINISHED, DefaultInputConsumableDecider.Factory.INSTANCE);
        assertAndLoadInputConsumableDecider(
                CAN_CONSUME_PARTIAL_FINISHED,
                PartialFinishedInputConsumableDecider.Factory.INSTANCE);
        assertAndLoadInputConsumableDecider(
                ONLY_CONSUME_ALL_FINISHED, AllFinishedInputConsumableDecider.Factory.INSTANCE);
    }

    private void assertAndLoadInputConsumableDecider(
            HybridConsumePartitionMode hybridConsumePartitionMode,
            InputConsumableDecider.Factory expectedFactory) {
        InputConsumableDecider.Factory factory =
                AdaptiveBatchSchedulerFactory.loadInputConsumableDeciderFactory(
                        hybridConsumePartitionMode);
        assertThat(factory).isEqualTo(expectedFactory);
    }
}
