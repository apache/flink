/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.StreamSourceContexts;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskTestHarness;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.TimerService;
import org.apache.flink.streaming.util.CollectorOutput;
import org.apache.flink.streaming.util.MockStreamTask;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/** Tests for {@link StreamSource} operators. */
@SuppressWarnings("serial")
class StreamSourceOperatorWatermarksTest {

    @Test
    void testEmitMaxWatermarkForFiniteSource() throws Exception {
        StreamSource<String, ?> sourceOperator = new StreamSource<>(new FiniteSource<>());
        StreamTaskTestHarness<String> testHarness =
                setupSourceStreamTask(sourceOperator, BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.invoke();
        testHarness.waitForTaskCompletion();

        assertThat(testHarness.getOutput()).hasSize(1);
        assertThat(testHarness.getOutput().peek()).isEqualTo(Watermark.MAX_WATERMARK);
    }

    @Test
    void testDisabledProgressiveWatermarksForFiniteSource() throws Exception {
        StreamSource<String, ?> sourceOperator =
                new StreamSource<>(new FiniteSourceWithWatermarks<>(), false);
        StreamTaskTestHarness<String> testHarness =
                setupSourceStreamTask(sourceOperator, BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.invoke();
        testHarness.waitForTaskCompletion();

        // sent by source function
        assertThat(testHarness.getOutput().poll()).isEqualTo(Watermark.MAX_WATERMARK);

        // sent by framework
        assertThat(testHarness.getOutput().poll()).isEqualTo(Watermark.MAX_WATERMARK);

        assertThat(testHarness.getOutput()).isEmpty();
    }

    @Test
    void testNoMaxWatermarkOnImmediateCancel() throws Exception {
        StreamSource<String, ?> sourceOperator = new StreamSource<>(new InfiniteSource<>());
        StreamTaskTestHarness<String> testHarness =
                setupSourceStreamTask(sourceOperator, BasicTypeInfo.STRING_TYPE_INFO, true);

        testHarness.invoke();
        assertThatThrownBy(testHarness::waitForTaskCompletion)
                .hasCauseInstanceOf(CancelTaskException.class);

        assertThat(testHarness.getOutput()).isEmpty();
    }

    @Test
    void testNoMaxWatermarkOnAsyncCancel() throws Exception {
        StreamSource<String, ?> sourceOperator = new StreamSource<>(new InfiniteSource<>());
        StreamTaskTestHarness<String> testHarness =
                setupSourceStreamTask(sourceOperator, BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.invoke();
        testHarness.waitForTaskRunning();
        Thread.sleep(200);
        testHarness.getTask().cancel();
        try {
            testHarness.waitForTaskCompletion();
        } catch (Throwable t) {
            if (!ExceptionUtils.findThrowable(t, CancelTaskException.class).isPresent()) {
                throw t;
            }
        }
        assertThat(testHarness.getOutput()).isEmpty();
    }

    @Test
    void testAutomaticWatermarkContext() throws Exception {

        // regular stream source operator
        final StreamSource<String, InfiniteSource<String>> operator =
                new StreamSource<>(new InfiniteSource<>());

        long watermarkInterval = 10;
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        processingTimeService.setCurrentTime(0);

        MockStreamTask<?, ?> task =
                setupSourceOperator(
                        operator,
                        TimeCharacteristic.IngestionTime,
                        watermarkInterval,
                        processingTimeService);

        final List<StreamElement> output = new ArrayList<>();

        StreamSourceContexts.getSourceContext(
                TimeCharacteristic.IngestionTime,
                processingTimeService,
                task.getCheckpointLock(),
                new CollectorOutput<String>(output),
                operator.getExecutionConfig().getAutoWatermarkInterval(),
                -1,
                true);

        // periodically emit the watermarks
        // even though we start from 1 the watermark are still
        // going to be aligned with the watermark interval.

        for (long i = 1; i < 100; i += watermarkInterval) {
            processingTimeService.setCurrentTime(i);
        }

        assertThat(output).hasSize(9);

        long nextWatermark = 0;
        for (StreamElement el : output) {
            nextWatermark += watermarkInterval;
            Watermark wm = (Watermark) el;
            assertThat(wm.getTimestamp()).isEqualTo(nextWatermark);
        }
    }

    // ------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static <T> MockStreamTask setupSourceOperator(
            StreamSource<T, ?> operator,
            TimeCharacteristic timeChar,
            long watermarkInterval,
            final TimerService timeProvider)
            throws Exception {

        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setAutoWatermarkInterval(watermarkInterval);

        StreamConfig cfg = new StreamConfig(new Configuration());
        cfg.setStateBackend(new MemoryStateBackend());

        cfg.setTimeCharacteristic(timeChar);
        cfg.setOperatorID(new OperatorID());

        Environment env = new DummyEnvironment("MockTwoInputTask", 1, 0);

        MockStreamTask mockTask =
                new MockStreamTaskBuilder(env)
                        .setConfig(cfg)
                        .setExecutionConfig(executionConfig)
                        .setTimerService(timeProvider)
                        .build();

        operator.setup(mockTask, cfg, (Output<StreamRecord<T>>) mock(Output.class));
        return mockTask;
    }

    private static <T> StreamTaskTestHarness<T> setupSourceStreamTask(
            StreamSource<T, ?> sourceOperator, TypeInformation<T> outputType) {

        return setupSourceStreamTask(sourceOperator, outputType, false);
    }

    private static <T> StreamTaskTestHarness<T> setupSourceStreamTask(
            StreamSource<T, ?> sourceOperator,
            TypeInformation<T> outputType,
            final boolean cancelImmediatelyAfterCreation) {

        final StreamTaskTestHarness<T> testHarness =
                new StreamTaskTestHarness<>(
                        (env) -> {
                            SourceStreamTask<T, ?, ?> sourceTask = new SourceStreamTask<>(env);
                            if (cancelImmediatelyAfterCreation) {
                                try {
                                    sourceTask.cancel();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                            return sourceTask;
                        },
                        outputType);
        testHarness.setupOutputForSingletonOperatorChain();

        StreamConfig streamConfig = testHarness.getStreamConfig();
        streamConfig.setStreamOperator(sourceOperator);
        streamConfig.setOperatorID(new OperatorID());
        streamConfig.setTimeCharacteristic(TimeCharacteristic.EventTime);

        return testHarness;
    }

    // ------------------------------------------------------------------------

    private static final class FiniteSource<T> extends RichSourceFunction<T> {

        @Override
        public void run(SourceContext<T> ctx) {}

        @Override
        public void cancel() {}
    }

    private static final class FiniteSourceWithWatermarks<T> extends RichSourceFunction<T> {

        @Override
        public void run(SourceContext<T> ctx) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.emitWatermark(new Watermark(1000));
                ctx.emitWatermark(new Watermark(2000));
                ctx.emitWatermark(Watermark.MAX_WATERMARK);
            }
        }

        @Override
        public void cancel() {}
    }

    private static final class InfiniteSource<T> implements SourceFunction<T> {

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<T> ctx) throws Exception {
            while (running) {
                Thread.sleep(20);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
