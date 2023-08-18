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
 * limitations under the License
 */

package org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease;

import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingPipelinedRegion;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ConsumerRegionGroupExecutionView} and {@link
 * ConsumerRegionGroupExecutionViewMaintainer}.
 */
class ConsumerRegionGroupExecutionViewMaintainerTest {

    private TestingSchedulingPipelinedRegion producerRegion;
    private TestingSchedulingPipelinedRegion consumerRegion;

    private ConsumerRegionGroupExecutionView consumerRegionGroupExecutionView;
    private ConsumerRegionGroupExecutionViewMaintainer consumerRegionGroupExecutionViewMaintainer;

    @BeforeEach
    void setup() {
        createProducerAndConsumer();
        createConsumerRegionGroupExecutionViewMaintainer();
    }

    @Test
    void testRegionFinished() throws Exception {
        consumerRegionGroupExecutionViewMaintainer.regionFinished(consumerRegion);
        assertThat(consumerRegionGroupExecutionView.isFinished()).isTrue();
    }

    @Test
    void testRegionUnfinished() throws Exception {
        consumerRegionGroupExecutionViewMaintainer.regionFinished(consumerRegion);
        consumerRegionGroupExecutionViewMaintainer.regionUnfinished(consumerRegion);

        assertThat(consumerRegionGroupExecutionView.isFinished()).isFalse();
    }

    @Test
    void testRegionFinishedMultipleTimes() throws Exception {
        consumerRegionGroupExecutionViewMaintainer.regionFinished(consumerRegion);
        consumerRegionGroupExecutionViewMaintainer.regionFinished(consumerRegion);

        assertThat(consumerRegionGroupExecutionView.isFinished()).isTrue();
    }

    @Test
    void testRegionUnfinishedMultipleTimes() throws Exception {
        consumerRegionGroupExecutionViewMaintainer.regionUnfinished(consumerRegion);
        consumerRegionGroupExecutionViewMaintainer.regionUnfinished(consumerRegion);

        assertThat(consumerRegionGroupExecutionView.isFinished()).isFalse();

        consumerRegionGroupExecutionViewMaintainer.regionFinished(consumerRegion);
        assertThat(consumerRegionGroupExecutionView.isFinished()).isTrue();
    }

    @Test
    void testFinishWrongRegion() {
        consumerRegionGroupExecutionViewMaintainer.regionFinished(producerRegion);
        assertThat(consumerRegionGroupExecutionView.isFinished()).isFalse();
    }

    @Test
    void testUnfinishedWrongRegion() {
        consumerRegionGroupExecutionViewMaintainer.regionUnfinished(producerRegion);
        assertThat(consumerRegionGroupExecutionView.isFinished()).isFalse();
    }

    private void createProducerAndConsumer() {
        TestingSchedulingExecutionVertex producer =
                TestingSchedulingExecutionVertex.newBuilder().build();
        TestingSchedulingExecutionVertex consumer =
                TestingSchedulingExecutionVertex.newBuilder().build();

        producerRegion = new TestingSchedulingPipelinedRegion(Collections.singleton(producer));
        consumerRegion = new TestingSchedulingPipelinedRegion(Collections.singleton(consumer));
    }

    private void createConsumerRegionGroupExecutionViewMaintainer() {
        consumerRegionGroupExecutionView = new ConsumerRegionGroupExecutionView();
        consumerRegionGroupExecutionView.add(consumerRegion);

        consumerRegionGroupExecutionViewMaintainer =
                new ConsumerRegionGroupExecutionViewMaintainer();
        consumerRegionGroupExecutionViewMaintainer.notifyNewRegionGroupExecutionViews(
                Collections.singletonList(consumerRegionGroupExecutionView));
    }
}
