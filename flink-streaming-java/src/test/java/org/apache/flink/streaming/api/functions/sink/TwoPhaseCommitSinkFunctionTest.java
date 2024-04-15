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

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.ContentDump;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.event.Level;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TwoPhaseCommitSinkFunction}. */
class TwoPhaseCommitSinkFunctionTest {

    private ContentDumpSinkFunction sinkFunction;

    private OneInputStreamOperatorTestHarness<String, Object> harness;

    private AtomicBoolean throwException = new AtomicBoolean();

    private ContentDump targetDirectory;

    private ContentDump tmpDirectory;

    private SettableClock clock;

    @RegisterExtension
    private LoggerAuditingExtension testLoggerResource =
            new LoggerAuditingExtension(TwoPhaseCommitSinkFunction.class, Level.WARN);

    @BeforeEach
    void setUp() throws Exception {
        targetDirectory = new ContentDump();
        tmpDirectory = new ContentDump();
        clock = new SettableClock();

        setUpTestHarness();
    }

    @AfterEach
    void tearDown() throws Exception {
        closeTestHarness();
    }

    private void setUpTestHarness() throws Exception {
        sinkFunction = new ContentDumpSinkFunction();
        harness =
                new OneInputStreamOperatorTestHarness<>(
                        new StreamSink<>(sinkFunction), StringSerializer.INSTANCE);
        harness.setup();
    }

    private void closeTestHarness() throws Exception {
        harness.close();
    }

    /**
     * This can happen if savepoint and checkpoint are triggered one after another and checkpoints
     * completes first. See FLINK-10377 and FLINK-14979 for more details.
     */
    @Test
    void testSubsumedNotificationOfPreviousCheckpoint() throws Exception {
        harness.open();
        harness.processElement("42", 0);
        harness.snapshot(0, 1);
        harness.processElement("43", 2);
        harness.snapshot(1, 3);
        harness.processElement("44", 4);
        harness.snapshot(2, 5);
        harness.notifyOfCompletedCheckpoint(2);
        harness.notifyOfCompletedCheckpoint(1);

        assertExactlyOnce(Arrays.asList("42", "43", "44"));
        assertThat(tmpDirectory.listFiles()).hasSize(1); // one for currentTransaction
    }

    @Test
    void testNoTransactionAfterSinkFunctionFinish() throws Exception {
        harness.open();
        harness.processElement("42", 0);
        harness.snapshot(0, 1);
        harness.processElement("43", 2);
        harness.snapshot(1, 3);
        harness.processElement("44", 4);

        // do not expect new input after finish()
        sinkFunction.finish();

        harness.snapshot(2, 5);
        harness.notifyOfCompletedCheckpoint(1);

        // make sure the previous empty transaction will not be pre-committed
        harness.snapshot(3, 6);

        assertThatThrownBy(() -> harness.processElement("45", 7))
                .isInstanceOf(NullPointerException.class);

        // Checkpoint2 has not complete
        assertExactlyOnce(Arrays.asList("42", "43"));

        // transaction for checkpoint2
        assertThat(tmpDirectory.listFiles()).hasSize(1);
    }

    @Test
    void testRecoverFromStateAfterFinished() throws Exception {
        harness.open();
        harness.processElement("42", 0);
        sinkFunction.finish();

        OperatorSubtaskState operatorSubtaskState = harness.snapshot(2, 5);

        closeTestHarness();
        setUpTestHarness();

        harness.initializeState(operatorSubtaskState);
        harness.open();
        assertThat(sinkFunction.abortedTransactions).isEmpty();
    }

    @Test
    void testNotifyOfCompletedCheckpoint() throws Exception {
        harness.open();
        harness.processElement("42", 0);
        harness.snapshot(0, 1);
        harness.processElement("43", 2);
        harness.snapshot(1, 3);
        harness.processElement("44", 4);
        harness.snapshot(2, 5);
        harness.notifyOfCompletedCheckpoint(1);

        assertExactlyOnce(Arrays.asList("42", "43"));
        // one for checkpointId 2 and second for the currentTransaction
        assertThat(tmpDirectory.listFiles()).hasSize(2);
    }

    @Test
    void testFailBeforeNotify() throws Exception {
        harness.open();
        harness.processElement("42", 0);
        harness.snapshot(0, 1);
        harness.processElement("43", 2);
        OperatorSubtaskState snapshot = harness.snapshot(1, 3);

        tmpDirectory.setWritable(false);

        assertThatThrownBy(
                        () -> {
                            harness.processElement("44", 4);
                            harness.snapshot(2, 5);
                        })
                .hasCauseInstanceOf(ContentDump.NotWritableException.class);

        closeTestHarness();

        tmpDirectory.setWritable(true);

        setUpTestHarness();
        harness.initializeState(snapshot);

        assertExactlyOnce(Arrays.asList("42", "43"));
        closeTestHarness();

        assertThat(tmpDirectory.listFiles()).isEmpty();
    }

    @Test
    void testIgnoreCommitExceptionDuringRecovery() throws Exception {
        clock.setEpochMilli(0);

        harness.open();
        harness.processElement("42", 0);

        final OperatorSubtaskState snapshot = harness.snapshot(0, 1);
        harness.notifyOfCompletedCheckpoint(1);

        throwException.set(true);

        closeTestHarness();
        setUpTestHarness();

        final long transactionTimeout = 1000;
        sinkFunction.setTransactionTimeout(transactionTimeout);
        sinkFunction.ignoreFailuresAfterTransactionTimeout();

        assertThatThrownBy(() -> harness.initializeState(snapshot))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Expected exception");

        clock.setEpochMilli(transactionTimeout + 1);
        harness.initializeState(snapshot);

        assertExactlyOnce(Collections.singletonList("42"));
    }

    @Test
    void testLogTimeoutAlmostReachedWarningDuringCommit() throws Exception {
        clock.setEpochMilli(0);

        final long transactionTimeout = 1000;
        final double warningRatio = 0.5;
        sinkFunction.setTransactionTimeout(transactionTimeout);
        sinkFunction.enableTransactionTimeoutWarnings(warningRatio);

        harness.open();
        harness.snapshot(0, 1);
        final long elapsedTime = (long) ((double) transactionTimeout * warningRatio + 2);
        clock.setEpochMilli(elapsedTime);
        harness.notifyOfCompletedCheckpoint(1);

        assertThat(testLoggerResource.getMessages())
                .anyMatch(
                        item ->
                                item.contains(
                                        "has been open for 502 ms. This is close to or even exceeding the transaction timeout of 1000 ms."));
    }

    @Test
    void testLogTimeoutAlmostReachedWarningDuringRecovery() throws Exception {
        clock.setEpochMilli(0);

        final long transactionTimeout = 1000;
        final double warningRatio = 0.5;
        sinkFunction.setTransactionTimeout(transactionTimeout);
        sinkFunction.enableTransactionTimeoutWarnings(warningRatio);

        harness.open();

        final OperatorSubtaskState snapshot = harness.snapshot(0, 1);
        final long elapsedTime = (long) ((double) transactionTimeout * warningRatio + 2);
        clock.setEpochMilli(elapsedTime);

        closeTestHarness();
        setUpTestHarness();
        sinkFunction.setTransactionTimeout(transactionTimeout);
        sinkFunction.enableTransactionTimeoutWarnings(warningRatio);

        harness.initializeState(snapshot);
        harness.open();

        closeTestHarness();

        assertThat(testLoggerResource.getMessages())
                .anyMatch(
                        item ->
                                item.contains(
                                        "has been open for 502 ms. This is close to or even exceeding the transaction timeout of 1000 ms."));
    }

    private void assertExactlyOnce(List<String> expectedValues) {
        ArrayList<String> actualValues = new ArrayList<>();
        for (String name : targetDirectory.listFiles()) {
            actualValues.addAll(targetDirectory.read(name));
        }
        Collections.sort(actualValues);
        Collections.sort(expectedValues);
        assertThat(actualValues).isEqualTo(expectedValues);
    }

    private class ContentDumpSinkFunction
            extends TwoPhaseCommitSinkFunction<String, ContentTransaction, Void> {
        final List<ContentTransaction> abortedTransactions = new ArrayList<>();

        public ContentDumpSinkFunction() {
            super(new ContentTransactionSerializer(), VoidSerializer.INSTANCE, clock);
        }

        @Override
        protected void invoke(ContentTransaction transaction, String value, Context context)
                throws Exception {
            transaction.tmpContentWriter.write(value);
        }

        @Override
        protected ContentTransaction beginTransaction() throws Exception {
            return new ContentTransaction(tmpDirectory.createWriter(UUID.randomUUID().toString()));
        }

        @Override
        protected void preCommit(ContentTransaction transaction) throws Exception {
            transaction.tmpContentWriter.flush();
            transaction.tmpContentWriter.close();
        }

        @Override
        protected void commit(ContentTransaction transaction) {
            if (throwException.get()) {
                throw new RuntimeException("Expected exception");
            }

            ContentDump.move(transaction.tmpContentWriter.getName(), tmpDirectory, targetDirectory);
        }

        @Override
        protected void abort(ContentTransaction transaction) {
            abortedTransactions.add(transaction);
            transaction.tmpContentWriter.close();
            tmpDirectory.delete(transaction.tmpContentWriter.getName());
        }
    }

    private static class ContentTransaction {
        private ContentDump.ContentWriter tmpContentWriter;

        public ContentTransaction(ContentDump.ContentWriter tmpContentWriter) {
            this.tmpContentWriter = tmpContentWriter;
        }

        @Override
        public String toString() {
            return String.format("ContentTransaction[%s]", tmpContentWriter.getName());
        }
    }

    private static class ContentTransactionSerializer extends KryoSerializer<ContentTransaction> {

        public ContentTransactionSerializer() {
            super(ContentTransaction.class, new SerializerConfigImpl());
        }

        @Override
        public KryoSerializer<ContentTransaction> duplicate() {
            return this;
        }

        @Override
        public String toString() {
            return "ContentTransactionSerializer";
        }
    }

    private static class SettableClock extends Clock {

        private final ZoneId zoneId;

        private long epochMilli;

        private SettableClock() {
            this.zoneId = ZoneOffset.UTC;
        }

        public SettableClock(ZoneId zoneId, long epochMilli) {
            this.zoneId = zoneId;
            this.epochMilli = epochMilli;
        }

        public void setEpochMilli(long epochMilli) {
            this.epochMilli = epochMilli;
        }

        @Override
        public ZoneId getZone() {
            return zoneId;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            if (zone.equals(this.zoneId)) {
                return this;
            }
            return new SettableClock(zone, epochMilli);
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(epochMilli);
        }
    }
}
