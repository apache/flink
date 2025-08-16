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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskTestHarness;
import org.apache.flink.test.util.source.AbstractTestSource;
import org.apache.flink.test.util.source.TestSourceReader;
import org.apache.flink.test.util.source.TestSplit;
import org.apache.flink.test.util.source.TestSplitEnumerator;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for Source V2 operators. */
class StreamSourceOperatorWatermarksTest {

    @Test
    void testEmitMaxWatermarkForFiniteSource() throws Exception {
        SourceOperatorFactory<String> factory =
                new SourceOperatorFactory<>(
                        finiteSource(), WatermarkStrategy.noWatermarks(), false, 1);
        StreamTaskTestHarness<String> testHarness =
                setupSourceOperatorTask(factory, BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.invoke();
        testHarness.waitForTaskCompletion();

        assertThat(testHarness.getOutput()).hasSize(1);
        assertThat(testHarness.getOutput().peek())
                .isEqualTo(new org.apache.flink.streaming.api.watermark.Watermark(Long.MAX_VALUE));
    }

    @Test
    void testDisabledProgressiveWatermarksForFiniteSource() throws Exception {
        SourceOperatorFactory<String> factory =
                new SourceOperatorFactory<>(
                        finiteSourceWithSelfEmittedWMs(),
                        WatermarkStrategy.noWatermarks(),
                        true,
                        1);
        StreamTaskTestHarness<String> testHarness =
                setupSourceOperatorTask(factory, BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.invoke();
        testHarness.waitForTaskCompletion();

        // sent by source function
        assertThat(testHarness.getOutput().poll())
                .isEqualTo(new org.apache.flink.streaming.api.watermark.Watermark(Long.MAX_VALUE));

        // sent by framework
        assertThat(testHarness.getOutput().poll())
                .isEqualTo(new org.apache.flink.streaming.api.watermark.Watermark(Long.MAX_VALUE));

        assertThat(testHarness.getOutput()).isEmpty();
    }

    @Test
    void testNoMaxWatermarkOnImmediateCancel() throws Exception {
        SourceOperatorFactory<String> factory =
                new SourceOperatorFactory<>(
                        infiniteSource(), WatermarkStrategy.noWatermarks(), false, 1);
        StreamTaskTestHarness<String> testHarness =
                setupSourceOperatorTask(factory, BasicTypeInfo.STRING_TYPE_INFO, true);

        testHarness.invoke();
        assertThatThrownBy(testHarness::waitForTaskCompletion)
                .hasCauseInstanceOf(CancelTaskException.class);

        assertThat(testHarness.getOutput()).isEmpty();
    }

    @Test
    void testNoMaxWatermarkOnAsyncCancel() throws Exception {
        SourceOperatorFactory<String> factory =
                new SourceOperatorFactory<>(
                        infiniteSource(), WatermarkStrategy.noWatermarks(), false, 1);
        StreamTaskTestHarness<String> testHarness =
                setupSourceOperatorTask(factory, BasicTypeInfo.STRING_TYPE_INFO);

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

    private static <T> StreamTaskTestHarness<T> setupSourceOperatorTask(
            SourceOperatorFactory<T> factory, TypeInformation<T> outputType) {
        return setupSourceOperatorTask(factory, outputType, false);
    }

    private static <T> StreamTaskTestHarness<T> setupSourceOperatorTask(
            SourceOperatorFactory<T> factory,
            TypeInformation<T> outputType,
            boolean cancelImmediately) {

        final StreamTaskTestHarness<T> testHarness =
                new StreamTaskTestHarness<>(
                        (env) -> {
                            SourceOperatorStreamTask<T> sourceOperatorStreamTask =
                                    new SourceOperatorStreamTask<>(env);
                            if (cancelImmediately) {
                                sourceOperatorStreamTask.cancel();
                            }
                            return sourceOperatorStreamTask;
                        },
                        outputType);
        testHarness.setupOutputForSingletonOperatorChain();

        StreamConfig cfg = testHarness.getStreamConfig();
        cfg.setStreamOperatorFactory(factory);
        cfg.setOperatorID(new OperatorID());
        return testHarness;
    }

    /**
     * Creates a simple split enumerator that assigns one split and optionally signals completion.
     */
    private static SplitEnumerator<TestSplit, Void> createSimpleEnumerator(
            SplitEnumeratorContext<TestSplit> context, boolean signalNoMoreSplits) {
        return new TestSplitEnumerator<>(context, null) {
            private boolean assigned = false;

            @Override
            public void addReader(int subtaskId) {
                if (!assigned) {
                    context.assignSplit(TestSplit.INSTANCE, subtaskId);
                    assigned = true;
                }
                if (signalNoMoreSplits) {
                    context.signalNoMoreSplits(subtaskId);
                }
            }
        };
    }

    /** Finite (bounded) source → framework emits MAX_WM. */
    private static AbstractTestSource<String> finiteSource() {
        return new AbstractTestSource<>() {
            @Override
            public SourceReader<String, TestSplit> createReader(SourceReaderContext ctx) {
                return new TestSourceReader<>(ctx) {
                    private boolean done = false;

                    @Override
                    public InputStatus pollNext(ReaderOutput<String> out) {
                        if (!done) {
                            done = true;
                            return InputStatus.END_OF_INPUT;
                        }
                        return InputStatus.NOTHING_AVAILABLE;
                    }
                };
            }

            @Override
            public SplitEnumerator<TestSplit, Void> createEnumerator(
                    SplitEnumeratorContext<TestSplit> context) {
                return createSimpleEnumerator(context, true);
            }
        };
    }

    /** Finite source that self-emits WM_MAX (then framework also emits MAX_WM). */
    private static AbstractTestSource<String> finiteSourceWithSelfEmittedWMs() {
        return new AbstractTestSource<>() {
            @Override
            public SourceReader<String, TestSplit> createReader(SourceReaderContext ctx) {
                return new TestSourceReader<String>(ctx) {
                    private int step = 0;

                    @Override
                    public InputStatus pollNext(ReaderOutput<String> out) {
                        if (step == 0) {
                            out.emitWatermark(new Watermark(Long.MAX_VALUE));
                            step = 1;
                            return InputStatus.NOTHING_AVAILABLE;
                        } else if (step == 1) {
                            step = 2;
                            return InputStatus.END_OF_INPUT; // bounded completion
                        }
                        return InputStatus.NOTHING_AVAILABLE;
                    }
                };
            }

            @Override
            public SplitEnumerator<TestSplit, Void> createEnumerator(
                    SplitEnumeratorContext<TestSplit> context) {
                return createSimpleEnumerator(context, true); // bounded → framework MAX_WM too
            }
        };
    }

    /** Infinite (unbounded) source → no framework MAX_WM on cancel. */
    private static AbstractTestSource<String> infiniteSource() {
        return new AbstractTestSource<>() {
            @Override
            public SourceReader<String, TestSplit> createReader(SourceReaderContext ctx) {
                return new TestSourceReader<>(ctx) {
                    @Override
                    public InputStatus pollNext(ReaderOutput<String> out) {
                        return InputStatus.NOTHING_AVAILABLE; // idle forever until cancel
                    }
                };
            }

            @Override
            public SplitEnumerator<TestSplit, Void> createEnumerator(
                    SplitEnumeratorContext<TestSplit> context) {
                return createSimpleEnumerator(context, false); // unbounded → no signalNoMoreSplits
            }
        };
    }
}
