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

package org.apache.flink.streaming.connectors.dynamodb;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.dynamodb.config.AWSConfigConstants;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.InstantiationUtil;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Suite of {@link DynamoDbSink} tests. */
public class DynamoDbSinkTest {

    @Test
    public void testSinkIsSerializable() {
        final DummyDynamoDbSink<String> sink =
                new DummyDynamoDbSink<>(new DummySinkFunction(), getStandardProperties());
        assertTrue(InstantiationUtil.isSerializable(sink));
    }

    /**
     * Tests that any batch failure in the listener callbacks is rethrown on an immediately
     * following invoke call.
     */
    @Test
    public void testBatchFailureRethrownOnInvoke() throws Throwable {
        final DummyDynamoDbSink<String> sink =
                new DummyDynamoDbSink<>(new DummySinkFunction(), getStandardProperties());

        final OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();

        // setup the next batch request
        testHarness.processElement(new StreamRecord<>("msg"));
        verify(sink.getMockBatchProcessor(), times(1)).produce(Mockito.any(PutItemRequest.class));

        // manually execute the next batch request and complete with an exception
        sink.manualCompletePendingRequest(new Exception("artificial failure for batch request"));
        try {
            testHarness.processElement(new StreamRecord<>("next msg"));
        } catch (Exception e) {
            // the invoke should have failed with the batch request failure
            Assert.assertTrue(
                    e.getCause().getMessage().contains("artificial failure for batch request"));

            // test succeeded
            return;
        }

        Assert.fail();
    }

    /**
     * Tests that any batch failure in the listener callbacks is rethrown on an immediately
     * following checkpoint.
     */
    @Test
    public void testBatchFailureRethrownOnCheckpoint() throws Throwable {
        final DummyDynamoDbSink<String> sink =
                new DummyDynamoDbSink<>(new DummySinkFunction(), getStandardProperties());

        final OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();

        // setup the next batch request
        testHarness.processElement(new StreamRecord<>("msg"));
        verify(sink.getMockBatchProcessor(), times(1)).produce(Mockito.any(PutItemRequest.class));
        // manually execute the next batch request and complete with an exception
        sink.manualCompletePendingRequest(new Exception("artificial failure for batch request"));

        try {
            testHarness.snapshot(1L, 1000L);
        } catch (Exception e) {
            // the snapshot should have failed with the batch request failure
            Assert.assertTrue(
                    e.getCause()
                            .getCause()
                            .getMessage()
                            .contains("artificial failure for batch request"));

            // test succeeded
            return;
        }

        Assert.fail();
    }

    /**
     * Tests that any batch failure in the listener callbacks is rethrown on an immediately
     * following close.
     */
    @Test
    public void testBatchFailureRethrownOnClose() throws Throwable {
        final DummyDynamoDbSink<String> sink =
                new DummyDynamoDbSink<>(new DummySinkFunction(), getStandardProperties());

        final OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();

        // setup the next batch request, and let the whole batch request fail
        testHarness.processElement(new StreamRecord<>("msg"));
        verify(sink.getMockBatchProcessor(), times(1)).produce(Mockito.any(PutItemRequest.class));

        // manually execute the next batch request
        sink.manualCompletePendingRequest(new Exception("artificial failure for batch request"));

        try {
            testHarness.close();
        } catch (Exception e) {
            // the snapshot should have failed with the batch request failure
            Assert.assertTrue(
                    e.getCause().getMessage().contains("artificial failure for batch request"));

            // test succeeded
            return;
        }

        Assert.fail();
    }

    /**
     * Tests that any batch failure in the listener callbacks due to flushing on an immediately
     * following checkpoint is rethrown; we set a timeout because the test will not finish if the
     * logic is broken.
     */
    @Test(timeout = 10000)
    public void testBatchFailureRethrownOnCheckpointAfterFlush() throws Throwable {
        final DummyDynamoDbSink<String> sink =
                new DummyDynamoDbSink<>(new DummySinkFunction(), getStandardProperties());

        final OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();

        testHarness.processElement(new StreamRecord<>("msg-1"));
        verify(sink.getMockBatchProcessor(), times(1)).produce(Mockito.any(PutItemRequest.class));

        // manually execute the next batch request
        sink.manualCompletePendingRequest(null);

        // setup the requests to be flushed in the snapshot
        testHarness.processElement(new StreamRecord<>("msg-2"));
        testHarness.processElement(new StreamRecord<>("msg-3"));
        verify(sink.getMockBatchProcessor(), times(3)).produce(Mockito.any(PutItemRequest.class));

        CheckedThread snapshotThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        testHarness.snapshot(1L, 1000L);
                    }
                };
        snapshotThread.start();

        // for the snapshot-triggered flush, we let the batch request fail completely
        sink.manualCompletePendingRequest(new Exception("artificial failure for batch request"));

        try {
            snapshotThread.sync();
        } catch (Exception e) {
            // the snapshot should have failed with the batch request failure
            Assert.assertTrue(
                    e.getCause()
                            .getCause()
                            .getCause()
                            .getMessage()
                            .contains("artificial failure for batch request"));

            // test succeeded
            return;
        }

        Assert.fail();
    }

    /**
     * Tests that the sink correctly waits for pending requests on checkpoints; we set a timeout
     * because the test will not finish if the logic is broken.
     */
    @Test(timeout = 10000)
    public void testAtLeastOnceSink() throws Throwable {
        final DummyDynamoDbSink<String> sink =
                new DummyDynamoDbSink<>(new DummySinkFunction(), getStandardProperties());

        final OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();

        testHarness.processElement(new StreamRecord<>("msg-1"));
        testHarness.processElement(new StreamRecord<>("msg-2"));
        testHarness.processElement(new StreamRecord<>("msg-3"));
        verify(sink.getMockBatchProcessor(), times(3)).produce(Mockito.any(PutItemRequest.class));

        CheckedThread snapshotThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        testHarness.snapshot(1L, 1000L);
                    }
                };
        snapshotThread.start();

        // before proceeding, make sure that flushing has started and that the snapshot is still
        // blocked;
        // this would block forever if the snapshot didn't perform a flush
        sink.waitUntilFlushStarted();

        Assert.assertTrue(
                "Snapshot returned before all records were flushed", snapshotThread.isAlive());

        sink.manualCompletePendingRequest(null);
        Assert.assertTrue(
                "Snapshot returned before all records were flushed", snapshotThread.isAlive());

        sink.manualCompletePendingRequest(null);
        Assert.assertTrue(
                "Snapshot returned before all records were flushed", snapshotThread.isAlive());

        sink.manualCompletePendingRequest(null);

        // the snapshot should finish with no exceptions
        snapshotThread.sync();

        testHarness.close();
    }

    /**
     * Test ensuring that the producer blocks if the queue limit is exceeded, until the queue length
     * drops below the limit; we set a timeout because the test will not finish if the logic is
     * broken.
     */
    @Test(timeout = 10000)
    public void testBackpressure() throws Throwable {
        final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(10));

        final DummyDynamoDbSink<String> sink =
                new DummyDynamoDbSink<>(new DummySinkFunction(), getStandardProperties());
        sink.setQueueLimit(1);

        OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();

        CheckedThread msg1 =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        testHarness.processElement(new StreamRecord<>("msg-1"));
                    }
                };
        msg1.start();
        msg1.trySync(deadline.timeLeftIfAny().toMillis());
        assertFalse("Flush triggered before reaching queue limit", msg1.isAlive());

        sink.manualCompletePendingRequest(null);

        CheckedThread msg2 =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        testHarness.processElement(new StreamRecord<>("msg-2"));
                    }
                };
        msg2.start();
        msg2.trySync(deadline.timeLeftIfAny().toMillis());
        assertFalse("Flush triggered before reaching queue limit", msg2.isAlive());

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

        assertTrue("Producer should still block, but doesn't", moreElementsThread.isAlive());

        while (sink.getSize() < 2) {
            Thread.sleep(50);
        }

        sink.manualCompletePendingRequest(null);

        assertTrue("Sink should still block, but doesn't", moreElementsThread.isAlive());

        // consume msg-3, blocked msg-4 can be inserted into the queue and block is released
        while (sink.getSize() < 3) {
            Thread.sleep(50);
        }

        sink.manualCompletePendingRequest(null);

        moreElementsThread.trySync(deadline.timeLeftIfAny().toMillis());

        assertFalse(
                "Producer still blocks although the queue is flushed",
                moreElementsThread.isAlive());

        sink.manualCompletePendingRequest(null);

        testHarness.close();
    }

    private static class DummyDynamoDbSink<T> extends DynamoDbSink<T> {

        private static final long serialVersionUID = 4785071393265352746L;

        private transient DynamoDbProducer mockProducer;
        private transient List<PutItemRequest> batchRequests = new ArrayList<>();
        private transient MultiShotLatch flushLatch = new MultiShotLatch();
        private DynamoDbProducer.Listener listener;

        private int completed = 0;

        public DummyDynamoDbSink(
                DynamoDbSinkFunction<T> dynamoDBSinkFunction, Properties configProps) {
            super(dynamoDBSinkFunction, configProps);
        }

        /**
         * This method is used to mimic a scheduled batch request; we need to do this manually
         * because we are mocking the BatchProcessor.
         */
        public void waitUntilFlushStarted() throws Exception {
            flushLatch.await();
        }

        /**
         * This method is used to mimic the completion of a request towards DynamoDB. This method
         * will trigger the listener callback with an exception if throwable is not null
         *
         * @param throwable
         */
        public void manualCompletePendingRequest(Throwable throwable) {
            completed++;
            batchRequests.get(completed - 1);
            ProducerWriteRequest producerWriteRequest =
                    new ProducerWriteRequest("", "", ImmutableList.of());
            listener.beforeWrite("123", producerWriteRequest);

            if (throwable == null) {
                listener.afterWrite(
                        "123",
                        producerWriteRequest,
                        new ProducerWriteResponse("123", true, 2, null, 10L));
            } else {
                listener.afterWrite("123L", producerWriteRequest, throwable);
            }
        }

        public DynamoDbProducer getMockBatchProcessor() {
            return mockProducer;
        }

        public long getOutstandingRecordsCount() {
            return batchRequests.size() - completed;
        }

        public int getSize() {
            return batchRequests.size();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

            super.snapshotState(context);

            // if the snapshot implementation doesn't wait until all pending records are flushed, we
            // should fail the test
            if (mockProducer.getOutstandingRecordsCount() > 0) {
                throw new RuntimeException(
                        "Snapshots should be blocked until all pending records are flushed");
            }
        }

        /**
         * Override the batch processor build process to provide a mock implementation, but reuse
         * the listener implementation in our mock to test that the listener logic works correctly
         * with request flushing logic.
         */
        @Override
        protected DynamoDbProducer buildDynamoDBProducer(final DynamoDbProducer.Listener listener) {
            this.listener = listener;
            this.mockProducer = mock(DynamoDbProducer.class);

            doAnswer(
                            new Answer<Object>() {
                                @Override
                                public Object answer(InvocationOnMock invocationOnMock)
                                        throws Throwable {
                                    // intercept the request and add it to our mock batch requests
                                    batchRequests.add(invocationOnMock.getArgument(0));
                                    return null;
                                }
                            })
                    .when(mockProducer)
                    .produce(Mockito.any(PutItemRequest.class));

            doAnswer(
                            new Answer<Object>() {
                                @Override
                                public Long answer(InvocationOnMock invocation) throws Throwable {
                                    return getOutstandingRecordsCount();
                                }
                            })
                    .when(mockProducer)
                    .getOutstandingRecordsCount();

            try {
                doAnswer(
                                new Answer<Object>() {
                                    @Override
                                    public Object answer(InvocationOnMock invocationOnMock)
                                            throws Throwable {
                                        // wait until we are allowed to continue with the flushing
                                        flushLatch.trigger();
                                        return null;
                                    }
                                })
                        .when(mockProducer)
                        .flush();
            } catch (Exception e) {
                e.printStackTrace();
            }

            return mockProducer;
        }
    }

    private static class DummySinkFunction implements DynamoDbSinkFunction<String> {

        private static final long serialVersionUID = 289642437354078271L;

        @Override
        public void process(String element, RuntimeContext ctx, DynamoDbProducer producer) {
            producer.produce(PutItemRequest.builder().build());
        }
    }

    private static Properties getStandardProperties() {
        Properties config = new Properties();
        config.setProperty(AWSConfigConstants.AWS_REGION, "eu-west-1");
        return config;
    }
}
