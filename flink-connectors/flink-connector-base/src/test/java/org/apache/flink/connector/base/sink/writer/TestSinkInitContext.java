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

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Callable;

/** A mock implementation of a {@code Sink.InitContext} to be used in sink unit tests. */
public class TestSinkInitContext implements Sink.InitContext {

    private static final TestProcessingTimeService processingTimeService;
    private final MetricListener metricListener = new MetricListener();
    private final OperatorIOMetricGroup operatorIOMetricGroup =
            UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup().getIOMetricGroup();
    private final SinkWriterMetricGroup metricGroup =
            InternalSinkWriterMetricGroup.mock(
                    metricListener.getMetricGroup(), operatorIOMetricGroup);

    static {
        processingTimeService = new TestProcessingTimeService();
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return null;
    }

    @Override
    public MailboxExecutor getMailboxExecutor() {
        StreamTaskActionExecutor streamTaskActionExecutor =
                new StreamTaskActionExecutor() {
                    @Override
                    public void run(RunnableWithException e) throws Exception {
                        e.run();
                    }

                    @Override
                    public <E extends Throwable> void runThrowing(
                            ThrowingRunnable<E> throwingRunnable) throws E {
                        throwingRunnable.run();
                    }

                    @Override
                    public <R> R call(Callable<R> callable) throws Exception {
                        return callable.call();
                    }
                };
        return new MailboxExecutorImpl(
                new TaskMailboxImpl(Thread.currentThread()),
                Integer.MAX_VALUE,
                streamTaskActionExecutor);
    }

    @Override
    public Sink.ProcessingTimeService getProcessingTimeService() {
        return new Sink.ProcessingTimeService() {
            @Override
            public long getCurrentProcessingTime() {
                return processingTimeService.getCurrentProcessingTime();
            }

            @Override
            public void registerProcessingTimer(
                    long time, ProcessingTimeCallback processingTimerCallback) {
                processingTimeService.registerTimer(
                        time, processingTimerCallback::onProcessingTime);
            }
        };
    }

    @Override
    public int getSubtaskId() {
        return 0;
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return 0;
    }

    @Override
    public SinkWriterMetricGroup metricGroup() {
        return metricGroup;
    }

    @Override
    public OptionalLong getRestoredCheckpointId() {
        return OptionalLong.empty();
    }

    public TestProcessingTimeService getTestProcessingTimeService() {
        return processingTimeService;
    }

    public Optional<Gauge<Long>> getCurrentSendTimeGauge() {
        return metricListener.getGauge("currentSendTime");
    }

    public Counter getNumRecordsOutCounter() {
        return metricGroup.getIOMetricGroup().getNumRecordsOutCounter();
    }

    public Counter getNumBytesOutCounter() {
        return metricGroup.getIOMetricGroup().getNumBytesOutCounter();
    }
}
