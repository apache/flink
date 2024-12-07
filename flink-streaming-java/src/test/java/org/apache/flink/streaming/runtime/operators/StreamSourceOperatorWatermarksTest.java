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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.functions.source.legacy.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskTestHarness;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    // ------------------------------------------------------------------------

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
