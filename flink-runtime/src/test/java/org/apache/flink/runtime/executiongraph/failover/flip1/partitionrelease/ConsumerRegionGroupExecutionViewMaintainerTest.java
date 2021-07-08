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
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ConsumerRegionGroupExecutionView} and {@link
 * ConsumerRegionGroupExecutionViewMaintainer}.
 */
public class ConsumerRegionGroupExecutionViewMaintainerTest extends TestLogger {

    private TestingSchedulingPipelinedRegion producerRegion;
    private TestingSchedulingPipelinedRegion consumerRegion;

    private ConsumerRegionGroupExecutionView consumerRegionGroupExecutionView;
    private ConsumerRegionGroupExecutionViewMaintainer consumerRegionGroupExecutionViewMaintainer;

    @Before
    public void setup() {
        createProducerAndConsumer();
        createConsumerRegionGroupExecutionViewMaintainer();
    }

    @Test
    public void testRegionFinished() throws Exception {
        consumerRegionGroupExecutionViewMaintainer.regionFinished(consumerRegion);
        assertTrue(consumerRegionGroupExecutionView.isFinished());
    }

    @Test
    public void testRegionUnfinished() throws Exception {
        consumerRegionGroupExecutionViewMaintainer.regionFinished(consumerRegion);
        consumerRegionGroupExecutionViewMaintainer.regionUnfinished(consumerRegion);

        assertFalse(consumerRegionGroupExecutionView.isFinished());
    }

    @Test
    public void testRegionFinishedMultipleTimes() throws Exception {
        consumerRegionGroupExecutionViewMaintainer.regionFinished(consumerRegion);
        consumerRegionGroupExecutionViewMaintainer.regionFinished(consumerRegion);

        assertTrue(consumerRegionGroupExecutionView.isFinished());
    }

    @Test
    public void testRegionUnfinishedMultipleTimes() throws Exception {
        consumerRegionGroupExecutionViewMaintainer.regionUnfinished(consumerRegion);
        consumerRegionGroupExecutionViewMaintainer.regionUnfinished(consumerRegion);

        assertFalse(consumerRegionGroupExecutionView.isFinished());

        consumerRegionGroupExecutionViewMaintainer.regionFinished(consumerRegion);
        assertTrue(consumerRegionGroupExecutionView.isFinished());
    }

    @Test
    public void testFinishWrongRegion() {
        consumerRegionGroupExecutionViewMaintainer.regionFinished(producerRegion);
        assertFalse(consumerRegionGroupExecutionView.isFinished());
    }

    @Test
    public void testUnfinishedWrongRegion() {
        consumerRegionGroupExecutionViewMaintainer.regionUnfinished(producerRegion);
        assertFalse(consumerRegionGroupExecutionView.isFinished());
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
                new ConsumerRegionGroupExecutionViewMaintainer(
                        Collections.singletonList(consumerRegionGroupExecutionView));
    }
}
