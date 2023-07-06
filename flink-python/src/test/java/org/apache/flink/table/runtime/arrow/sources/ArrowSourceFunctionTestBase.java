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

package org.apache.flink.table.runtime.arrow.sources;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.arrow.ArrowWriter;
import org.apache.flink.testutils.CustomEqualityMatcher;
import org.apache.flink.testutils.DeeplyEqualsChecker;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.assertj.core.api.HamcrestCondition;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Abstract test base for the Arrow source function processing. */
abstract class ArrowSourceFunctionTestBase {

    final VectorSchemaRoot root;
    private final TypeSerializer<RowData> typeSerializer;
    private final Comparator<RowData> comparator;
    private final DeeplyEqualsChecker checker;

    ArrowSourceFunctionTestBase(
            VectorSchemaRoot root,
            TypeSerializer<RowData> typeSerializer,
            Comparator<RowData> comparator) {
        this(root, typeSerializer, comparator, new DeeplyEqualsChecker());
    }

    ArrowSourceFunctionTestBase(
            VectorSchemaRoot root,
            TypeSerializer<RowData> typeSerializer,
            Comparator<RowData> comparator,
            DeeplyEqualsChecker checker) {
        this.root = Preconditions.checkNotNull(root);
        this.typeSerializer = Preconditions.checkNotNull(typeSerializer);
        this.comparator = Preconditions.checkNotNull(comparator);
        this.checker = Preconditions.checkNotNull(checker);
    }

    @Test
    void testRestore() throws Exception {
        Tuple2<List<RowData>, Integer> testData = getTestData();
        final ArrowSourceFunction arrowSourceFunction =
                createTestArrowSourceFunction(testData.f0, testData.f1);

        final AbstractStreamOperatorTestHarness<RowData> testHarness =
                new AbstractStreamOperatorTestHarness<>(
                        new StreamSource<>(arrowSourceFunction), 1, 1, 0);
        testHarness.open();

        final Throwable[] error = new Throwable[1];
        final MultiShotLatch latch = new MultiShotLatch();
        final AtomicInteger numOfEmittedElements = new AtomicInteger(0);
        final List<RowData> results = new ArrayList<>();

        final DummySourceContext<RowData> sourceContext =
                new DummySourceContext<RowData>() {
                    @Override
                    public void collect(RowData element) {
                        if (numOfEmittedElements.get() == 2) {
                            latch.trigger();
                            // fail the source function at the second element
                            throw new RuntimeException("Fail the arrow source");
                        }
                        results.add(typeSerializer.copy(element));
                        numOfEmittedElements.incrementAndGet();
                    }
                };

        // run the source asynchronously
        Thread runner =
                new Thread(
                        () -> {
                            try {
                                arrowSourceFunction.run(sourceContext);
                            } catch (Throwable t) {
                                if (!t.getMessage().equals("Fail the arrow source")) {
                                    error[0] = t;
                                }
                            }
                        });
        runner.start();

        if (!latch.isTriggered()) {
            latch.await();
        }

        OperatorSubtaskState snapshot;
        synchronized (sourceContext.getCheckpointLock()) {
            snapshot = testHarness.snapshot(0, 0);
        }

        runner.join();
        testHarness.close();

        final ArrowSourceFunction arrowSourceFunction2 =
                createTestArrowSourceFunction(testData.f0, testData.f1);
        AbstractStreamOperatorTestHarness testHarnessCopy =
                new AbstractStreamOperatorTestHarness(
                        new StreamSource<>(arrowSourceFunction2), 1, 1, 0);
        testHarnessCopy.initializeState(snapshot);
        testHarnessCopy.open();

        // run the source asynchronously
        Thread runner2 =
                new Thread(
                        () -> {
                            try {
                                arrowSourceFunction2.run(
                                        new DummySourceContext<RowData>() {
                                            @Override
                                            public void collect(RowData element) {
                                                results.add(typeSerializer.copy(element));
                                                if (numOfEmittedElements.incrementAndGet()
                                                        == testData.f0.size()) {
                                                    latch.trigger();
                                                }
                                            }
                                        });
                            } catch (Throwable t) {
                                error[0] = t;
                            }
                        });
        runner2.start();

        if (!latch.isTriggered()) {
            latch.await();
        }
        runner2.join();

        assertThat(error[0]).isNull();
        assertThat(testData.f0).hasSize(numOfEmittedElements.get());
        checkElementsEquals(results, testData.f0);
    }

    @Test
    void testParallelProcessing() throws Exception {
        Tuple2<List<RowData>, Integer> testData = getTestData();
        final ArrowSourceFunction arrowSourceFunction =
                createTestArrowSourceFunction(testData.f0, testData.f1);

        final AbstractStreamOperatorTestHarness<RowData> testHarness =
                new AbstractStreamOperatorTestHarness(
                        new StreamSource<>(arrowSourceFunction), 2, 2, 0);
        testHarness.open();

        final Throwable[] error = new Throwable[2];
        final OneShotLatch latch = new OneShotLatch();
        final AtomicInteger numOfEmittedElements = new AtomicInteger(0);
        final List<RowData> results = Collections.synchronizedList(new ArrayList<>());

        // run the source asynchronously
        Thread runner =
                new Thread(
                        () -> {
                            try {
                                arrowSourceFunction.run(
                                        new DummySourceContext<RowData>() {
                                            @Override
                                            public void collect(RowData element) {
                                                results.add(typeSerializer.copy(element));
                                                if (numOfEmittedElements.incrementAndGet()
                                                        == testData.f0.size()) {
                                                    latch.trigger();
                                                }
                                            }
                                        });
                            } catch (Throwable t) {
                                error[0] = t;
                            }
                        });
        runner.start();

        final ArrowSourceFunction arrowSourceFunction2 =
                createTestArrowSourceFunction(testData.f0, testData.f1);
        final AbstractStreamOperatorTestHarness<RowData> testHarness2 =
                new AbstractStreamOperatorTestHarness(
                        new StreamSource<>(arrowSourceFunction2), 2, 2, 1);
        testHarness2.open();

        // run the source asynchronously
        Thread runner2 =
                new Thread(
                        () -> {
                            try {
                                arrowSourceFunction2.run(
                                        new DummySourceContext<RowData>() {
                                            @Override
                                            public void collect(RowData element) {
                                                results.add(typeSerializer.copy(element));
                                                if (numOfEmittedElements.incrementAndGet()
                                                        == testData.f0.size()) {
                                                    latch.trigger();
                                                }
                                            }
                                        });
                            } catch (Throwable t) {
                                error[1] = t;
                            }
                        });
        runner2.start();

        if (!latch.isTriggered()) {
            latch.await();
        }

        runner.join();
        runner2.join();
        testHarness.close();
        testHarness2.close();

        assertThat(error[0]).isNull();
        assertThat(error[1]).isNull();
        assertThat(testData.f0).hasSize(numOfEmittedElements.get());
        checkElementsEquals(results, testData.f0);
    }

    public abstract Tuple2<List<RowData>, Integer> getTestData();

    public abstract ArrowWriter<RowData> createArrowWriter();

    public abstract ArrowSourceFunction createArrowSourceFunction(byte[][] arrowData);

    private void checkElementsEquals(List<RowData> actual, List<RowData> expected) {
        assertThat(actual).hasSize(expected.size());
        actual.sort(comparator);
        expected.sort(comparator);
        for (int i = 0; i < expected.size(); i++) {
            assertThat(actual.get(i))
                    .is(
                            HamcrestCondition.matching(
                                    CustomEqualityMatcher.deeplyEquals(expected.get(i))
                                            .withChecker(checker)));
        }
    }

    /** Create continuous monitoring function with 1 reader-parallelism and interval. */
    private ArrowSourceFunction createTestArrowSourceFunction(List<RowData> testData, int batches)
            throws IOException {
        ArrowWriter<RowData> arrowWriter = createArrowWriter();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(root, null, baos);
        arrowStreamWriter.start();
        List<List<RowData>> subLists = Lists.partition(testData, testData.size() / batches + 1);
        for (List<RowData> subList : subLists) {
            for (RowData value : subList) {
                arrowWriter.write(value);
            }
            arrowWriter.finish();
            arrowStreamWriter.writeBatch();
            arrowWriter.reset();
        }

        ArrowSourceFunction arrowSourceFunction =
                createArrowSourceFunction(
                        ArrowUtils.readArrowBatches(
                                Channels.newChannel(new ByteArrayInputStream(baos.toByteArray()))));
        arrowSourceFunction.setRuntimeContext(new MockStreamingRuntimeContext(false, 0, 0));
        return arrowSourceFunction;
    }

    private abstract static class DummySourceContext<T> implements SourceFunction.SourceContext<T> {

        private final Object lock = new Object();

        @Override
        public void collectWithTimestamp(T element, long timestamp) {}

        @Override
        public void emitWatermark(Watermark mark) {}

        @Override
        public void markAsTemporarilyIdle() {}

        @Override
        public Object getCheckpointLock() {
            return lock;
        }

        @Override
        public void close() {}
    }
}
