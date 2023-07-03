/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.taskmanager.TestCheckpointResponder;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.assertj.core.api.AbstractDoubleAssert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.assertj.core.api.Assertions.assertThat;

/** Common base class for testing source tasks. */
public class SourceStreamTaskTestBase {
    public void testMetrics(
            FunctionWithException<Environment, ? extends StreamTask<Integer, ?>, Exception>
                    taskFactory,
            StreamOperatorFactory<?> operatorFactory,
            Consumer<AbstractDoubleAssert<?>> busyTimeMatcher)
            throws Exception {
        long sleepTime = 42;

        StreamTaskMailboxTestHarnessBuilder<Integer> builder =
                new StreamTaskMailboxTestHarnessBuilder<>(taskFactory, INT_TYPE_INFO);

        final Map<String, Metric> metrics = new ConcurrentHashMap<>();
        final TaskMetricGroup taskMetricGroup =
                StreamTaskTestHarness.createTaskMetricGroup(metrics);

        try (StreamTaskMailboxTestHarness<Integer> harness =
                builder.setupOutputForSingletonOperatorChain(operatorFactory)
                        .setTaskMetricGroup(taskMetricGroup)
                        .build()) {

            Future<Boolean> triggerFuture =
                    harness.streamTask.triggerCheckpointAsync(
                            new CheckpointMetaData(1L, System.currentTimeMillis()),
                            CheckpointOptions.forCheckpointWithDefaultLocation());

            OneShotLatch checkpointAcknowledgeLatch = new OneShotLatch();
            harness.getCheckpointResponder().setAcknowledgeLatch(checkpointAcknowledgeLatch);

            assertThat(triggerFuture).isNotDone();
            Thread.sleep(sleepTime);
            while (!triggerFuture.isDone()) {
                harness.streamTask.runMailboxStep();
            }
            Gauge<Long> checkpointStartDelayGauge =
                    (Gauge<Long>) metrics.get(MetricNames.CHECKPOINT_START_DELAY_TIME);
            assertThat(checkpointStartDelayGauge.getValue())
                    .isGreaterThanOrEqualTo(sleepTime * 1_000_000);
            Gauge<Double> busyTimeGauge = (Gauge<Double>) metrics.get(MetricNames.TASK_BUSY_TIME);
            busyTimeMatcher.accept(assertThat(busyTimeGauge.getValue()));

            checkpointAcknowledgeLatch.await();
            TestCheckpointResponder.AcknowledgeReport acknowledgeReport =
                    Iterables.getOnlyElement(
                            harness.getCheckpointResponder().getAcknowledgeReports());
            assertThat(acknowledgeReport.getCheckpointMetrics().getCheckpointStartDelayNanos())
                    .isGreaterThanOrEqualTo(sleepTime * 1_000_000);
        }
    }
}
