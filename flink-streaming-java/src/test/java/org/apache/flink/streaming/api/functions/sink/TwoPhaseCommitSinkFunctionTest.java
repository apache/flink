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
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
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
import java.util.stream.Collectors;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link TwoPhaseCommitSinkFunction}.
 */
public class TwoPhaseCommitSinkFunctionTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	private FileBasedSinkFunction sinkFunction;

	private OneInputStreamOperatorTestHarness<String, Object> harness;

	private AtomicBoolean throwException = new AtomicBoolean();

	private File targetDirectory;

	private File tmpDirectory;

	private SettableClock clock;

	private Logger logger;

	private AppenderSkeleton testAppender;

	private List<LoggingEvent> loggingEvents;

	@Before
	public void setUp() throws Exception {
		loggingEvents = new ArrayList<>();
		setupLogger();

		targetDirectory = folder.newFolder("_target");
		tmpDirectory = folder.newFolder("_tmp");
		clock = new SettableClock();

		setUpTestHarness();
	}

	/**
	 * Setup {@link org.apache.log4j.Logger}, the default logger implementation for tests,
	 * to append {@link LoggingEvent}s to {@link #loggingEvents} so that we can assert if
	 * the right messages were logged.
	 *
	 * @see #testLogTimeoutAlmostReachedWarningDuringCommit
	 * @see #testLogTimeoutAlmostReachedWarningDuringRecovery
	 */
	private void setupLogger() {
		Logger.getRootLogger().removeAllAppenders();
		logger = Logger.getLogger(TwoPhaseCommitSinkFunction.class);
		testAppender = new AppenderSkeleton() {
			@Override
			protected void append(LoggingEvent event) {
				loggingEvents.add(event);
			}

			@Override
			public void close() {

			}

			@Override
			public boolean requiresLayout() {
				return false;
			}
		};
		logger.addAppender(testAppender);
		logger.setLevel(Level.WARN);
	}

	@After
	public void tearDown() throws Exception {
		closeTestHarness();
		if (logger != null) {
			logger.removeAppender(testAppender);
		}
		loggingEvents = null;
	}

	private void setUpTestHarness() throws Exception {
		sinkFunction = new FileBasedSinkFunction();
		harness = new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sinkFunction), StringSerializer.INSTANCE);
		harness.setup();
	}

	private void closeTestHarness() throws Exception {
		harness.close();
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
		assertEquals(2, tmpDirectory.listFiles().length); // one for checkpointId 2 and second for the currentTransaction
	}

	@Test
	public void testFailBeforeNotify() throws Exception {
		harness.open();
		harness.processElement("42", 0);
		harness.snapshot(0, 1);
		harness.processElement("43", 2);
		OperatorStateHandles snapshot = harness.snapshot(1, 3);

		assertTrue(tmpDirectory.setWritable(false));
		try {
			harness.processElement("44", 4);
			harness.snapshot(2, 5);
			fail("something should fail");
		}
		catch (Exception ex) {
			if (!(ex.getCause() instanceof FileNotFoundException)) {
				throw ex;
			}
			// ignore
		}
		closeTestHarness();

		assertTrue(tmpDirectory.setWritable(true));

		setUpTestHarness();
		harness.initializeState(snapshot);

		assertExactlyOnce(Arrays.asList("42", "43"));
		closeTestHarness();

		assertEquals(0, tmpDirectory.listFiles().length);
	}

	@Test
	public void testIgnoreCommitExceptionDuringRecovery() throws Exception {
		clock.setEpochMilli(0);

		harness.open();
		harness.processElement("42", 0);

		final OperatorStateHandles snapshot = harness.snapshot(0, 1);
		harness.notifyOfCompletedCheckpoint(1);

		final long transactionTimeout = 1000;
		sinkFunction.setTransactionTimeout(transactionTimeout);
		sinkFunction.ignoreFailuresAfterTransactionTimeout();
		throwException.set(true);

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

		final List<String> logMessages =
			loggingEvents.stream().map(LoggingEvent::getRenderedMessage).collect(Collectors.toList());

		assertThat(
			logMessages,
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

		final OperatorStateHandles snapshot = harness.snapshot(0, 1);
		final long elapsedTime = (long) ((double) transactionTimeout * warningRatio + 2);
		clock.setEpochMilli(elapsedTime);
		harness.initializeState(snapshot);

		final List<String> logMessages =
			loggingEvents.stream().map(LoggingEvent::getRenderedMessage).collect(Collectors.toList());

		assertThat(
			logMessages,
			hasItem(containsString("has been open for 502 ms. " +
				"This is close to or even exceeding the transaction timeout of 1000 ms.")));
	}

	private void assertExactlyOnce(List<String> expectedValues) throws IOException {
		ArrayList<String> actualValues = new ArrayList<>();
		for (File file : targetDirectory.listFiles()) {
			actualValues.addAll(Files.readAllLines(file.toPath(), Charset.defaultCharset()));
		}
		Collections.sort(actualValues);
		Collections.sort(expectedValues);
		assertEquals(expectedValues, actualValues);
	}

	private class FileBasedSinkFunction extends TwoPhaseCommitSinkFunction<String, FileTransaction, Void> {

		public FileBasedSinkFunction() {
			super(
				new KryoSerializer<>(FileTransaction.class, new ExecutionConfig()),
				VoidSerializer.INSTANCE, clock);

			if (!tmpDirectory.isDirectory() || !targetDirectory.isDirectory()) {
				throw new IllegalArgumentException();
			}
		}

		@Override
		protected void invoke(FileTransaction transaction, String value, Context context) throws Exception {
			transaction.writer.write(value);
		}

		@Override
		protected FileTransaction beginTransaction() throws Exception {
			File tmpFile = new File(tmpDirectory, UUID.randomUUID().toString());
			return new FileTransaction(tmpFile);
		}

		@Override
		protected void preCommit(FileTransaction transaction) throws Exception {
			transaction.writer.flush();
			transaction.writer.close();
		}

		@Override
		protected void commit(FileTransaction transaction) {
			if (throwException.get()) {
				throw new RuntimeException("Expected exception");
			}

			try {
				Files.move(
					transaction.tmpFile.toPath(),
					new File(targetDirectory, transaction.tmpFile.getName()).toPath(),
					ATOMIC_MOVE);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}

		}

		@Override
		protected void abort(FileTransaction transaction) {
			try {
				transaction.writer.close();
			} catch (IOException e) {
				// ignore
			}
			transaction.tmpFile.delete();
		}

		@Override
		protected void recoverAndAbort(FileTransaction transaction) {
			transaction.tmpFile.delete();
		}
	}

	private static class FileTransaction {
		private final File tmpFile;
		private final transient BufferedWriter writer;

		public FileTransaction(File tmpFile) throws IOException {
			this.tmpFile = tmpFile;
			this.writer = new BufferedWriter(new FileWriter(tmpFile));
		}

		@Override
		public String toString() {
			return String.format("FileTransaction[%s]", tmpFile.getName());
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
