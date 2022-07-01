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

package org.apache.flink.streaming.connectors.kinesis;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisSerializationSchema;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.MockSerializationSchema;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Suite of {@link FlinkKinesisProducer} tests. */
public class FlinkKinesisProducerTest extends TestLogger {

    // ----------------------------------------------------------------------
    // Tests to verify serializability
    // ----------------------------------------------------------------------

    @Test
    public void testCreateWithNonSerializableDeserializerFails() {
        assertThatThrownBy(
                        () -> {
                            new FlinkKinesisProducer<>(
                                    new NonSerializableSerializationSchema(),
                                    TestUtils.getStandardProperties());
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The provided serialization schema is not serializable");
    }

    @Test
    public void testCreateWithSerializableDeserializer() {
        new FlinkKinesisProducer<>(
                new SerializableSerializationSchema(), TestUtils.getStandardProperties());
    }

    @Test
    public void testConfigureWithNonSerializableCustomPartitionerFails() {
        assertThatThrownBy(
                        () -> {
                            new FlinkKinesisProducer<>(
                                            new SimpleStringSchema(),
                                            TestUtils.getStandardProperties())
                                    .setCustomPartitioner(new NonSerializableCustomPartitioner());
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The provided custom partitioner is not serializable");
    }

    @Test
    public void testConfigureWithSerializableCustomPartitioner() {
        new FlinkKinesisProducer<>(new SimpleStringSchema(), TestUtils.getStandardProperties())
                .setCustomPartitioner(new SerializableCustomPartitioner());
    }

    @Test
    public void testProducerIsSerializable() {
        FlinkKinesisProducer<String> producer =
                new FlinkKinesisProducer<>(
                        new SimpleStringSchema(), TestUtils.getStandardProperties());
        assertThat(InstantiationUtil.isSerializable(producer)).isTrue();
    }

    // ----------------------------------------------------------------------
    // Tests to verify at-least-once guarantee
    // ----------------------------------------------------------------------

    /**
     * Test ensuring that if an invoke call happens right after an async exception is caught, it
     * should be rethrown.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testAsyncErrorRethrownOnInvoke() throws Throwable {
        final DummyFlinkKinesisProducer<String> producer =
                new DummyFlinkKinesisProducer<>(new SimpleStringSchema());

        OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(producer));

        testHarness.open();

        testHarness.processElement(new StreamRecord<>("msg-1"));

        producer.getPendingRecordFutures()
                .get(0)
                .setException(new Exception("artificial async exception"));

        assertThatThrownBy(() -> testHarness.processElement(new StreamRecord<>("msg-2")))
                .hasStackTraceContaining("artificial async exception");
    }

    /**
     * Test ensuring that if a snapshot call happens right after an async exception is caught, it
     * should be rethrown.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testAsyncErrorRethrownOnCheckpoint() throws Throwable {
        final DummyFlinkKinesisProducer<String> producer =
                new DummyFlinkKinesisProducer<>(new SimpleStringSchema());

        OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(producer));

        testHarness.open();

        testHarness.processElement(new StreamRecord<>("msg-1"));

        producer.getPendingRecordFutures()
                .get(0)
                .setException(new Exception("artificial async exception"));

        assertThatThrownBy(() -> testHarness.snapshot(123L, 123L))
                .hasStackTraceContaining("artificial async exception");
    }

    /**
     * Test ensuring that if an async exception is caught for one of the flushed requests on
     * checkpoint, it should be rethrown; we set a timeout because the test will not finish if the
     * logic is broken.
     *
     * <p>Note that this test does not test the snapshot method is blocked correctly when there are
     * pending records. The test for that is covered in testAtLeastOnceProducer.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    @Timeout(10)
    public void testAsyncErrorRethrownAfterFlush() throws Throwable {
        final DummyFlinkKinesisProducer<String> producer =
                new DummyFlinkKinesisProducer<>(new SimpleStringSchema());

        OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(producer));

        testHarness.open();

        testHarness.processElement(new StreamRecord<>("msg-1"));
        testHarness.processElement(new StreamRecord<>("msg-2"));
        testHarness.processElement(new StreamRecord<>("msg-3"));

        // only let the first record succeed for now
        UserRecordResult result = mock(UserRecordResult.class);
        when(result.isSuccessful()).thenReturn(true);
        producer.getPendingRecordFutures().get(0).set(result);

        CheckedThread snapshotThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        // this should block at first, since there are still two pending records
                        // that needs to be flushed
                        testHarness.snapshot(123L, 123L);
                    }
                };
        snapshotThread.start();

        // let the 2nd message fail with an async exception
        producer.getPendingRecordFutures()
                .get(1)
                .setException(new Exception("artificial async failure for 2nd message"));
        producer.getPendingRecordFutures().get(2).set(mock(UserRecordResult.class));

        assertThatThrownBy(snapshotThread::sync)
                .hasStackTraceContaining("artificial async failure for 2nd message");
    }

    /**
     * Test ensuring that the producer is not dropping buffered records; we set a timeout because
     * the test will not finish if the logic is broken.
     */
    @SuppressWarnings({"unchecked", "ResultOfMethodCallIgnored"})
    @Test
    @Timeout(10)
    public void testAtLeastOnceProducer() throws Throwable {
        final DummyFlinkKinesisProducer<String> producer =
                new DummyFlinkKinesisProducer<>(new SimpleStringSchema());

        OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(producer));

        testHarness.open();

        testHarness.processElement(new StreamRecord<>("msg-1"));
        testHarness.processElement(new StreamRecord<>("msg-2"));
        testHarness.processElement(new StreamRecord<>("msg-3"));

        // start a thread to perform checkpointing
        CheckedThread snapshotThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        // this should block until all records are flushed;
                        // if the snapshot implementation returns before pending records are
                        // flushed,
                        testHarness.snapshot(123L, 123L);
                    }
                };
        snapshotThread.start();

        // before proceeding, make sure that flushing has started and that the snapshot is still
        // blocked;
        // this would block forever if the snapshot didn't perform a flush
        producer.waitUntilFlushStarted();
        assertThat(snapshotThread.isAlive())
                .as("Snapshot returned before all records were flushed")
                .isTrue();

        // now, complete the callbacks
        UserRecordResult result = mock(UserRecordResult.class);
        when(result.isSuccessful()).thenReturn(true);

        producer.getPendingRecordFutures().get(0).set(result);
        assertThat(snapshotThread.isAlive())
                .as("Snapshot returned before all records were flushed")
                .isTrue();

        producer.getPendingRecordFutures().get(1).set(result);
        assertThat(snapshotThread.isAlive())
                .as("Snapshot returned before all records were flushed")
                .isTrue();

        producer.getPendingRecordFutures().get(2).set(result);

        // this would fail with an exception if flushing wasn't completed before the snapshot method
        // returned
        snapshotThread.sync();

        testHarness.close();
    }

    /**
     * Test ensuring that the producer blocks if the queue limit is exceeded, until the queue length
     * drops below the limit; we set a timeout because the test will not finish if the logic is
     * broken.
     */
    @Test
    @Timeout(10)
    public void testBackpressure() throws Throwable {
        final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(10));

        final DummyFlinkKinesisProducer<String> producer =
                new DummyFlinkKinesisProducer<>(new SimpleStringSchema());
        producer.setQueueLimit(1);

        OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(producer));

        testHarness.open();

        UserRecordResult result = mock(UserRecordResult.class);
        when(result.isSuccessful()).thenReturn(true);

        CheckedThread msg1 =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        testHarness.processElement(new StreamRecord<>("msg-1"));
                    }
                };
        msg1.start();
        msg1.trySync(deadline.timeLeftIfAny().toMillis());
        assertThat(msg1.isAlive()).as("Flush triggered before reaching queue limit").isFalse();

        // consume msg-1 so that queue is empty again
        producer.getPendingRecordFutures().get(0).set(result);

        CheckedThread msg2 =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        testHarness.processElement(new StreamRecord<>("msg-2"));
                    }
                };
        msg2.start();
        msg2.trySync(deadline.timeLeftIfAny().toMillis());
        assertThat(msg2.isAlive()).as("Flush triggered before reaching queue limit").isFalse();

        CheckedThread moreElementsThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        // this should block until msg-2 is consumed
                        testHarness.processElement(new StreamRecord<>("msg-3"));
                        // this should block until msg-3 is consumed
                        testHarness.processElement(new StreamRecord<>("msg-4"));
                    }
                };
        moreElementsThread.start();

        assertThat(moreElementsThread.isAlive())
                .as("Producer should still block, but doesn't")
                .isTrue();

        // consume msg-2 from the queue, leaving msg-3 in the queue and msg-4 blocked
        while (producer.getPendingRecordFutures().size() < 2) {
            Thread.sleep(50);
        }
        producer.getPendingRecordFutures().get(1).set(result);

        assertThat(moreElementsThread.isAlive())
                .as("Producer should still block, but doesn't")
                .isTrue();

        // consume msg-3, blocked msg-4 can be inserted into the queue and block is released
        while (producer.getPendingRecordFutures().size() < 3) {
            Thread.sleep(50);
        }
        producer.getPendingRecordFutures().get(2).set(result);

        moreElementsThread.trySync(deadline.timeLeftIfAny().toMillis());

        assertThat(moreElementsThread.isAlive())
                .as("Prodcuer still blocks although the queue is flushed")
                .isFalse();

        producer.getPendingRecordFutures().get(3).set(result);

        testHarness.close();
    }

    @Test
    public void testOpen() throws Exception {
        MockSerializationSchema<Object> serializationSchema = new MockSerializationSchema<>();

        Properties config = TestUtils.getStandardProperties();
        FlinkKinesisProducer<Object> producer =
                new FlinkKinesisProducer<>(serializationSchema, config);
        AbstractStreamOperatorTestHarness<Object> testHarness =
                new AbstractStreamOperatorTestHarness<>(new StreamSink<>(producer), 1, 1, 0);

        testHarness.open();
        assertThat(serializationSchema.isOpenCalled()).as("Open method was not called").isTrue();
    }

    // ----------------------------------------------------------------------
    // Utility test classes
    // ----------------------------------------------------------------------

    /**
     * A non-serializable {@link KinesisSerializationSchema} (because it is a nested class with
     * reference to the enclosing class, which is not serializable) used for testing.
     */
    private final class NonSerializableSerializationSchema
            implements KinesisSerializationSchema<String> {

        private static final long serialVersionUID = 3361337188490178780L;

        @Override
        public ByteBuffer serialize(String element) {
            return ByteBuffer.wrap(element.getBytes());
        }

        @Override
        public String getTargetStream(String element) {
            return "test-stream";
        }
    }

    /** A static, serializable {@link KinesisSerializationSchema}. */
    private static final class SerializableSerializationSchema
            implements KinesisSerializationSchema<String> {

        private static final long serialVersionUID = 6298573834520052886L;

        @Override
        public ByteBuffer serialize(String element) {
            return ByteBuffer.wrap(element.getBytes());
        }

        @Override
        public String getTargetStream(String element) {
            return "test-stream";
        }
    }

    /**
     * A non-serializable {@link KinesisPartitioner} (because it is a nested class with reference to
     * the enclosing class, which is not serializable) used for testing.
     */
    private final class NonSerializableCustomPartitioner extends KinesisPartitioner<String> {

        private static final long serialVersionUID = -5961578876056779161L;

        @Override
        public String getPartitionId(String element) {
            return "test-partition";
        }
    }

    /** A static, serializable {@link KinesisPartitioner}. */
    private static final class SerializableCustomPartitioner extends KinesisPartitioner<String> {

        private static final long serialVersionUID = -4996071893997035695L;

        @Override
        public String getPartitionId(String element) {
            return "test-partition";
        }
    }

    private static class DummyFlinkKinesisProducer<T> extends FlinkKinesisProducer<T> {

        private static final long serialVersionUID = -1212425318784651817L;

        private static final String DUMMY_STREAM = "dummy-stream";
        private static final String DUMMY_PARTITION = "dummy-partition";

        private transient KinesisProducer mockProducer;
        private List<SettableFuture<UserRecordResult>> pendingRecordFutures = new LinkedList<>();

        private transient MultiShotLatch flushLatch;

        DummyFlinkKinesisProducer(SerializationSchema<T> schema) {
            super(schema, TestUtils.getStandardProperties());

            setDefaultStream(DUMMY_STREAM);
            setDefaultPartition(DUMMY_PARTITION);
            setFailOnError(true);

            // set up mock producer
            this.mockProducer = mock(KinesisProducer.class);

            when(mockProducer.addUserRecord(
                            anyString(),
                            anyString(),
                            nullable(String.class),
                            any(ByteBuffer.class)))
                    .thenAnswer(
                            new Answer<Object>() {
                                @Override
                                public Object answer(InvocationOnMock invocationOnMock)
                                        throws Throwable {
                                    SettableFuture<UserRecordResult> future =
                                            SettableFuture.create();
                                    pendingRecordFutures.add(future);
                                    return future;
                                }
                            });

            when(mockProducer.getOutstandingRecordsCount())
                    .thenAnswer(
                            new Answer<Object>() {
                                @Override
                                public Object answer(InvocationOnMock invocationOnMock)
                                        throws Throwable {
                                    return getNumPendingRecordFutures();
                                }
                            });

            doAnswer(
                            new Answer() {
                                @Override
                                public Object answer(InvocationOnMock invocationOnMock)
                                        throws Throwable {
                                    flushLatch.trigger();
                                    return null;
                                }
                            })
                    .when(mockProducer)
                    .flush();

            this.flushLatch = new MultiShotLatch();
        }

        @Override
        protected KinesisProducer getKinesisProducer(KinesisProducerConfiguration producerConfig) {
            return mockProducer;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

            super.snapshotState(context);

            // if the snapshot implementation doesn't wait until all pending records are flushed, we
            // should fail the test
            if (mockProducer.getOutstandingRecordsCount() > 0) {
                throw new RuntimeException(
                        "Flushing is enabled; snapshots should be blocked until all pending records are flushed");
            }
        }

        List<SettableFuture<UserRecordResult>> getPendingRecordFutures() {
            return pendingRecordFutures;
        }

        void waitUntilFlushStarted() throws Exception {
            flushLatch.await();
        }

        private int getNumPendingRecordFutures() {
            int numPending = 0;

            for (SettableFuture<UserRecordResult> future : pendingRecordFutures) {
                if (!future.isDone()) {
                    numPending++;
                }
            }

            return numPending;
        }
    }
}
