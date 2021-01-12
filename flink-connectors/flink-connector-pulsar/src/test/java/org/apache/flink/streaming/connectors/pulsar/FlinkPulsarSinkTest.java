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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchemaWrapper;
import org.apache.flink.util.TestLogger;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for sink.
 */
public class FlinkPulsarSinkTest extends TestLogger {

	/**
	 * Tests that the constructor eagerly checks bootstrap servers are set in config.
	 */
	@Test(timeout = 40 * 1000L, expected = IllegalArgumentException.class)
	public void testInstantiationFailsWhenServiceUrlMissing() throws Exception {
		new DummyFlinkPulsarSink<Long>(new ClientConfigurationData(), new Properties());
	}

	public static ClientConfigurationData dummyClientConf() {
		ClientConfigurationData clientConf = new ClientConfigurationData();
		clientConf.setServiceUrl("abc");
		return clientConf;
	}

	public static Properties dummyProperties() {
		Properties pros = new Properties();
		pros.setProperty(PulsarOptions.FAIL_ON_WRITE_OPTION_KEY, "true");
		return pros;
	}

	/**
	 * Test ensuring that if an invoke call happens right after an async exception is caught, it should be rethrown.
	 */
	@Test(timeout = 40 * 1000L)
	public void testAsyncErrorRethrownOnInvoke() throws Throwable {
		DummyFlinkPulsarSink<String> sink = new DummyFlinkPulsarSink<>(
			dummyClientConf(),
			dummyProperties());

		OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

		testHarness.open();

		testHarness.processElement(new StreamRecord<>("msg-1"));

		// let the message request return an async exception
		sink.getPendingCallbacks().get(0).accept(null, new Exception("artificial async exception"));

		try {
			testHarness.processElement(new StreamRecord<>("msg-2"));
		} catch (Exception e) {
			// the next invoke should rethrow the async exception
			Assert.assertTrue(e.getCause().getMessage().contains("artificial async exception"));

			// test succeeded
			return;
		}

		Assert.fail();
	}

	/**
	 * Test ensuring that if a snapshot call happens right after an async exception is caught, it should be rethrown.
	 */
	@Test(timeout = 40 * 1000L)
	public void testAsyncErrorRethrownOnCheckpoint() throws Throwable {
		final DummyFlinkPulsarSink<String> producer = new DummyFlinkPulsarSink<>(
			dummyClientConf(), dummyProperties());

		OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(producer));

		testHarness.open();

		testHarness.processElement(new StreamRecord<>("msg-1"));

		// let the message request return an async exception
		producer
			.getPendingCallbacks()
			.get(0)
			.accept(null, new Exception("artificial async exception"));

		try {
			testHarness.snapshot(123L, 123L);
		} catch (Exception e) {
			// the next invoke should rethrow the async exception
			Assert.assertTrue(e.getCause().getMessage().contains("artificial async exception"));

			// test succeeded
			return;
		}

		Assert.fail();
	}

	/**
	 * Test ensuring that if an async exception is caught for one of the flushed requests on checkpoint,
	 * it should be rethrown; we set a timeout because the test will not finish if the logic is broken.
	 *
	 * <p>Note that this test does not test the snapshot method is blocked correctly when there are pending records.
	 * The test for that is covered in testAtLeastOnceProducer.
	 */
	@SuppressWarnings("unchecked")
	@Test(timeout = 40 * 1000L)
	public void testAsyncErrorRethrownOnCheckpointAfterFlush() throws Throwable {
		final DummyFlinkPulsarSink<String> sink = new DummyFlinkPulsarSink<>(
			dummyClientConf(),
			dummyProperties());
		Producer mockProducer = sink.getProducer("tp");

		final OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

		testHarness.open();

		testHarness.processElement(new StreamRecord<>("msg-1"));
		testHarness.processElement(new StreamRecord<>("msg-2"));
		testHarness.processElement(new StreamRecord<>("msg-3"));

		verify(mockProducer, times(3)).newMessage();

		// only let the first callback succeed for now
		sink.getPendingCallbacks().get(0).accept(null, null);

		CheckedThread snapshotThread = new CheckedThread() {
			@Override
			public void go() throws Exception {
				// this should block at first, since there are still two pending records that needs to be flushed
				testHarness.snapshot(123L, 123L);
			}
		};
		snapshotThread.start();

		// let the 2nd message fail with an async exception
		sink
			.getPendingCallbacks()
			.get(1)
			.accept(null, new Exception("artificial async failure for 2nd message"));
		sink.getPendingCallbacks().get(2).accept(null, null);

		try {
			snapshotThread.sync();
		} catch (Exception e) {
			// the snapshot should have failed with the async exception
			Assert.assertTrue(e
				.getCause()
				.getMessage()
				.contains("artificial async failure for 2nd message"));

			// test succeeded
			return;
		}

		Assert.fail();
	}

	/**
	 * Test ensuring that the producer is not dropping buffered records;
	 * we set a timeout because the test will not finish if the logic is broken.
	 */
	@SuppressWarnings("unchecked")
	@Test(timeout = 10000)
	public void testAtLeastOnceProducer() throws Throwable {
		final DummyFlinkPulsarSink<String> sink = new DummyFlinkPulsarSink<>(
			dummyClientConf(),
			dummyProperties());

		final Producer mockProducer = sink.getProducer("tp");

		final OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

		testHarness.open();

		testHarness.processElement(new StreamRecord<>("msg-1"));
		testHarness.processElement(new StreamRecord<>("msg-2"));
		testHarness.processElement(new StreamRecord<>("msg-3"));

		verify(mockProducer, times(3));
		Assert.assertEquals(3, sink.getPendingSize());

		// start a thread to perform checkpointing
		CheckedThread snapshotThread = new CheckedThread() {
			@Override
			public void go() throws Exception {
				// this should block until all records are flushed;
				// if the snapshot implementation returns before pending records are flushed,
				testHarness.snapshot(123L, 123L);
			}
		};
		snapshotThread.start();

		// before proceeding, make sure that flushing has started and that the snapshot is still blocked;
		// this would block forever if the snapshot didn't perform a flush
		sink.waitUntilFlushStarted();
		Assert.assertTrue(
			"Snapshot returned before all records were flushed",
			snapshotThread.isAlive());

		// now, complete the callbacks
		sink.getPendingCallbacks().get(0).accept(null, null);
		Assert.assertTrue(
			"Snapshot returned before all records were flushed",
			snapshotThread.isAlive());
		Assert.assertEquals(2, sink.getPendingSize());

		sink.getPendingCallbacks().get(1).accept(null, null);
		Assert.assertTrue(
			"Snapshot returned before all records were flushed",
			snapshotThread.isAlive());
		Assert.assertEquals(1, sink.getPendingSize());

		sink.getPendingCallbacks().get(2).accept(null, null);
		Assert.assertEquals(0, sink.getPendingSize());

		// this would fail with an exception if flushing wasn't completed before the snapshot method returned
		snapshotThread.sync();

		testHarness.close();
	}

	/**
	 * This test is meant to assure that testAtLeastOnceProducer is valid by testing that if flushing is disabled,
	 * the snapshot method does indeed finishes without waiting for pending records;
	 * we set a timeout because the test will not finish if the logic is broken.
	 */
	@SuppressWarnings("unchecked")
	@Test(timeout = 40 * 1000L)//(timeout = 5000)
	public void testDoesNotWaitForPendingRecordsIfFlushingDisabled() throws Throwable {
		Properties props = dummyProperties();
		props.setProperty(PulsarOptions.FLUSH_ON_CHECKPOINT_OPTION_KEY, "false");

		final DummyFlinkPulsarSink<String> sink = new DummyFlinkPulsarSink<>(
			dummyClientConf(),
			props);

		final Producer mockProducer = sink.getProducer("tp");

		final OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

		testHarness.open();

		testHarness.processElement(new StreamRecord<>("msg"));

		// make sure that all callbacks have not been completed
		verify(mockProducer, times(1)).newMessage();

		// should return even if there are pending records
		testHarness.snapshot(123L, 123L);

		testHarness.close();
	}

	private static class DummyFlinkPulsarSink<T> extends FlinkPulsarSink<T> {

		private static final long serialVersionUID = 1L;

		private static final String DUMMY_TOPIC = "dummy-topic";

		private transient Producer<T> mockProducer;
		private transient TypedMessageBuilderImpl<T> mockMessageBuilder;

		public List<BiConsumer<MessageId, Throwable>> getPendingCallbacks() {
			return pendingCallbacks;
		}

		private transient List<BiConsumer<MessageId, Throwable>> pendingCallbacks;
		private transient MultiShotLatch flushLatch;
		private boolean isFlushed;

		public DummyFlinkPulsarSink(ClientConfigurationData clientConf, Properties properties) {
			super(
				"",
				Optional.of(DUMMY_TOPIC),
				clientConf,
				properties,
				new PulsarSerializationSchemaWrapper.Builder<>((SerializationSchema) element -> new byte[0])
					.useSpecialMode(Schema.STRING)
					.build());

			this.mockProducer = mock(Producer.class);
			this.mockMessageBuilder = mock(TypedMessageBuilderImpl.class);

			this.pendingCallbacks = new ArrayList<>();
			this.flushLatch = new MultiShotLatch();

			when(mockMessageBuilder.sendAsync()).thenAnswer((Answer<CompletableFuture<MessageId>>) invocation -> {

				CompletableFuture<MessageId> mockFuture = mock(CompletableFuture.class);

				when(mockFuture.whenComplete(any(BiConsumer.class))).thenAnswer(new Answer<T>() {
					@Override
					public T answer(InvocationOnMock i) throws Throwable {
						pendingCallbacks.add(i.getArgument(0));
						return null;
					}
				});
				return mockFuture;
			});

			when(mockMessageBuilder.value(any())).thenReturn(mockMessageBuilder);
			when(mockMessageBuilder.keyBytes(any())).thenReturn(mockMessageBuilder);
			when(mockMessageBuilder.eventTime(anyLong())).thenReturn(mockMessageBuilder);

			when(mockProducer.newMessage()).thenAnswer(new Answer<TypedMessageBuilderImpl<T>>() {
				@Override
				public TypedMessageBuilderImpl<T> answer(InvocationOnMock invocation) throws Throwable {
					return mockMessageBuilder;
				}
			});
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			isFlushed = false;
			super.snapshotState(context);
			// if the snapshot implementation doesn't wait until
			// all pending records are flushed, we should fail the test
			if (flushOnCheckpoint && !isFlushed) {
				throw new RuntimeException("Flushing is enabled; snapshots should be blocked" +
					" until all pending records are flushed");
			}
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			if (flushOnCheckpoint
				&& !((StreamingRuntimeContext) this.getRuntimeContext()).isCheckpointingEnabled()) {
				flushOnCheckpoint = false;
			}
		}

		public void waitUntilFlushStarted() throws InterruptedException {
			flushLatch.await();
		}

		@Override
		public void producerFlush(PulsarTransactionState<T> transaction) throws Exception {
			flushLatch.trigger();

			// simply wait until the producer's pending records become zero.
			// This relies on the fact that the producer's Callback implementation
			// and pending records tracking logic is implemented correctly, otherwise
			// we will loop forever.
			while (numPendingRecords() > 0) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					throw new RuntimeException("Unable to flush producer, task was interrupted");
				}
			}

			isFlushed = true;
		}

		@Override
		protected Producer<T> getProducer(String topic) {
			return mockProducer;
		}

		public long getPendingSize() {
			if (flushOnCheckpoint) {
				return numPendingRecords();
			} else {
				throw new UnsupportedOperationException("Get pending size");
			}
		}

		public long numPendingRecords() {
			synchronized (pendingRecordsLock) {
				return pendingRecords;
			}
		}
	}
}
