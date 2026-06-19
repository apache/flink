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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.JobManagerOptions.HybridPartitionDataConsumeConstraint;
import org.apache.flink.runtime.scheduler.strategy.AllFinishedInputConsumableDecider;
import org.apache.flink.runtime.scheduler.strategy.DefaultInputConsumableDecider;
import org.apache.flink.runtime.scheduler.strategy.InputConsumableDecider;
import org.apache.flink.runtime.scheduler.strategy.PartialFinishedInputConsumableDecider;

import org.junit.jupiter.api.Test;

import static org.apache.flink.configuration.JobManagerOptions.HybridPartitionDataConsumeConstraint.ALL_PRODUCERS_FINISHED;
import static org.apache.flink.configuration.JobManagerOptions.HybridPartitionDataConsumeConstraint.ONLY_FINISHED_PRODUCERS;
import static org.apache.flink.configuration.JobManagerOptions.HybridPartitionDataConsumeConstraint.UNFINISHED_PRODUCERS;
import static org.apache.flink.runtime.scheduler.adaptivebatch.AdaptiveBatchSchedulerFactory.getOrDecideHybridPartitionDataConsumeConstraint;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AdaptiveBatchSchedulerFactory}. */
class AdaptiveBatchSchedulerFactoryTest {
    @Test
    void testNotOnlyConsumeFinishedPartitionWithSpeculativeEnable() {
        Configuration configuration = new Configuration();
        configuration.set(
                JobManagerOptions.HYBRID_PARTITION_DATA_CONSUME_CONSTRAINT, UNFINISHED_PRODUCERS);
        assertThatThrownBy(
                        () -> getOrDecideHybridPartitionDataConsumeConstraint(configuration, true))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testOnlyConsumeFinishedPartitionWillSetForSpeculativeEnable() {
        HybridPartitionDataConsumeConstraint hybridPartitionDataConsumeConstraint =
                getOrDecideHybridPartitionDataConsumeConstraint(new Configuration(), true);
        assertThat(hybridPartitionDataConsumeConstraint.isOnlyConsumeFinishedPartition()).isTrue();
    }

    @Test
    void testLoadInputConsumableDeciderFactory() {
        assertAndLoadInputConsumableDecider(
                UNFINISHED_PRODUCERS, DefaultInputConsumableDecider.Factory.INSTANCE);
        assertAndLoadInputConsumableDecider(
                ONLY_FINISHED_PRODUCERS, PartialFinishedInputConsumableDecider.Factory.INSTANCE);
        assertAndLoadInputConsumableDecider(
                ALL_PRODUCERS_FINISHED, AllFinishedInputConsumableDecider.Factory.INSTANCE);
    }

    @Test
    void testMaxParallelismFallsBackToExecutionConfigDefaultParallelism() {
        Configuration configuration = new Configuration();
        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setParallelism(5);
        assertThat(
                        AdaptiveBatchSchedulerFactory.getDefaultMaxParallelism(
                                configuration, executionConfig))
                .isEqualTo(5);
    }

    private void assertAndLoadInputConsumableDecider(
            HybridPartitionDataConsumeConstraint hybridPartitionDataConsumeConstraint,
            InputConsumableDecider.Factory expectedFactory) {
        InputConsumableDecider.Factory factory =
                AdaptiveBatchSchedulerFactory.loadInputConsumableDeciderFactory(
                        hybridPartitionDataConsumeConstraint);
        assertThat(factory).isEqualTo(expectedFactory);
    }
}
