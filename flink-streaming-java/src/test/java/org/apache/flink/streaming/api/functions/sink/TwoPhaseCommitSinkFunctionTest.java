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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.ContentDump;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.testutils.logging.TestLoggerResource;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.event.Level;

import java.io.IOException;
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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link TwoPhaseCommitSinkFunction}.
 */
public class TwoPhaseCommitSinkFunctionTest {

	private ContentDumpSinkFunction sinkFunction;

	private OneInputStreamOperatorTestHarness<String, Object> harness;

	private AtomicBoolean throwException = new AtomicBoolean();

	private ContentDump targetDirectory;

	private ContentDump tmpDirectory;

	private SettableClock clock;

	@Rule
	public final TestLoggerResource testLoggerResource = new TestLoggerResource(TwoPhaseCommitSinkFunction.class, Level.WARN);

	@Before
	public void setUp() throws Exception {
		targetDirectory = new ContentDump();
		tmpDirectory = new ContentDump();
		clock = new SettableClock();

		setUpTestHarness();
	}

	@After
	public void tearDown() throws Exception {
		closeTestHarness();
	}

	private void setUpTestHarness() throws Exception {
		sinkFunction = new ContentDumpSinkFunction();
		harness = new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sinkFunction), StringSerializer.INSTANCE);
		harness.setup();
	}

	private void closeTestHarness() throws Exception {
		harness.close();
	}

	/**
	 * This can happen if savepoint and checkpoint are triggered one after another and checkpoints completes first.
	 * See FLINK-10377 and FLINK-14979 for more details.
	 **/
	@Test
	public void testSubsumedNotificationOfPreviousCheckpoint() throws Exception {
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
		assertEquals(1, tmpDirectory.listFiles().size()); // one for currentTransaction
	}

	@Test
	public void testNotifyOfCompletedCheckpoint() throws Exception {
		harness.open();
		harness.processElement("42", 0);
		harness.snapshot(0, 1);
		harness.processElement("43", 2);
		harness.snapshot(1, 3);
		harness.processElement("44", 4);
		harness.snapshot(2, 5);
		harness.notifyOfCompletedCheckpoint(1);

		assertExactlyOnce(Arrays.asList("42", "43"));
		assertEquals(2, tmpDirectory.listFiles().size()); // one for checkpointId 2 and second for the currentTransaction
	}

	@Test
	public void testFailBeforeNotify() throws Exception {
		harness.open();
		harness.processElement("42", 0);
		harness.snapshot(0, 1);
		harness.processElement("43", 2);
		OperatorSubtaskState snapshot = harness.snapshot(1, 3);

		tmpDirectory.setWritable(false);
		try {
			harness.processElement("44", 4);
			harness.snapshot(2, 5);
			fail("something should fail");
		} catch (Exception ex) {
			if (!(ex.getCause() instanceof ContentDump.NotWritableException)) {
				throw ex;
			}
			// ignore
		}
		closeTestHarness();

		tmpDirectory.setWritable(true);

		setUpTestHarness();
		harness.initializeState(snapshot);

		assertExactlyOnce(Arrays.asList("42", "43"));
		closeTestHarness();

		assertEquals(0, tmpDirectory.listFiles().size());
	}

	@Test
	public void testIgnoreCommitExceptionDuringRecovery() throws Exception {
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

		try {
			harness.initializeState(snapshot);
			fail("Expected exception not thrown");
		} catch (RuntimeException e) {
			assertEquals("Expected exception", e.getMessage());
		}

		clock.setEpochMilli(transactionTimeout + 1);
		harness.initializeState(snapshot);

		assertExactlyOnce(Collections.singletonList("42"));
	}

	@Test
	public void testLogTimeoutAlmostReachedWarningDuringCommit() throws Exception {
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

		assertThat(
			testLoggerResource.getMessages(),
			hasItem(containsString("has been open for 502 ms. " +
				"This is close to or even exceeding the transaction timeout of 1000 ms.")));
	}

	@Test
	public void testLogTimeoutAlmostReachedWarningDuringRecovery() throws Exception {
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

		assertThat(
			testLoggerResource.getMessages(),
			hasItem(containsString("has been open for 502 ms. " +
				"This is close to or even exceeding the transaction timeout of 1000 ms.")));
	}

	private void assertExactlyOnce(List<String> expectedValues) throws IOException {
		ArrayList<String> actualValues = new ArrayList<>();
		for (String name : targetDirectory.listFiles()) {
			actualValues.addAll(targetDirectory.read(name));
		}
		Collections.sort(actualValues);
		Collections.sort(expectedValues);
		assertEquals(expectedValues, actualValues);
	}

	private class ContentDumpSinkFunction extends TwoPhaseCommitSinkFunction<String, ContentTransaction, Void> {

		public ContentDumpSinkFunction() {
			super(
				new KryoSerializer<>(ContentTransaction.class, new ExecutionConfig()),
				VoidSerializer.INSTANCE, clock);
		}

		@Override
		protected void invoke(ContentTransaction transaction, String value, Context context) throws Exception {
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

			ContentDump.move(
				transaction.tmpContentWriter.getName(),
				tmpDirectory,
				targetDirectory);

		}

		@Override
		protected void abort(ContentTransaction transaction) {
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
