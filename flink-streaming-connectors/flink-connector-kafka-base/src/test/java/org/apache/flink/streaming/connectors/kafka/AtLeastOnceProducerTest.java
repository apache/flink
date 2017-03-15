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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.testutils.FakeStandardProducerConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test ensuring that the producer is not dropping buffered records
 */
@SuppressWarnings("unchecked")
public class AtLeastOnceProducerTest {

	/**
	 * Test ensuring that if an invoke call happens right after an async exception is caught, it should be rethrown
	 */
	@Test
	public void testAsyncErrorRethrownOnInvoke() throws Throwable {
		final DummyFlinkKafkaProducer<String> producer = new DummyFlinkKafkaProducer<>(
			FakeStandardProducerConfig.get(), null);

		OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(producer));

		testHarness.open();

		testHarness.processElement(new StreamRecord<>("msg-1"));

		// let the message request return an async exception
		producer.getPendingCallbacks().get(0).onCompletion(null, new Exception("artificial async exception"));

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
	 * Test ensuring that if a snapshot call happens right after an async exception is caught, it should be rethrown
	 */
	@Test
	public void testAsyncErrorRethrownOnCheckpoint() throws Throwable {
		final DummyFlinkKafkaProducer<String> producer = new DummyFlinkKafkaProducer<>(
			FakeStandardProducerConfig.get(), null);

		OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(producer));

		testHarness.open();

		testHarness.processElement(new StreamRecord<>("msg-1"));

		// let the message request return an async exception
		producer.getPendingCallbacks().get(0).onCompletion(null, new Exception("artificial async exception"));

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
	 * Note that this test does not test the snapshot method is blocked correctly when there are pending recorrds.
	 * The test for that is covered in testAtLeastOnceProducer.
	 */
	@SuppressWarnings("unchecked")
	@Test(timeout=5000)
	public void testAsyncErrorRethrownOnCheckpointAfterFlush() throws Throwable {
		final DummyFlinkKafkaProducer<String> producer = new DummyFlinkKafkaProducer<>(
			FakeStandardProducerConfig.get(), null);
		producer.setFlushOnCheckpoint(true);

		final KafkaProducer<?, ?> mockProducer = producer.getMockKafkaProducer();

		final OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(producer));

		testHarness.open();

		testHarness.processElement(new StreamRecord<>("msg-1"));
		testHarness.processElement(new StreamRecord<>("msg-2"));
		testHarness.processElement(new StreamRecord<>("msg-3"));

		verify(mockProducer, times(3)).send(any(ProducerRecord.class), any(Callback.class));

		// only let the first callback succeed for now
		producer.getPendingCallbacks().get(0).onCompletion(null, null);

		final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
		Thread snapshotThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					testHarness.snapshot(123L, 123L);
				} catch (Exception e) {
					exceptionRef.compareAndSet(null, e);
				}
			}
		});
		snapshotThread.start();

		// let the 2nd message fail with an async exception
		producer.getPendingCallbacks().get(1).onCompletion(null, new Exception("artificial async failure for 2nd message"));
		producer.getPendingCallbacks().get(2).onCompletion(null, null);

		snapshotThread.join();

		// the snapshot should have failed with the async exception
		Exception snapshotError = exceptionRef.get();
		assertTrue(snapshotError != null);
		assertTrue(snapshotError.getCause().getMessage().contains("artificial async failure for 2nd message"));
	}

	/**
	 * Test ensuring that the producer is not dropping buffered records;
	 * we set a timeout because the test will not finish if the logic is broken
	 */
	@SuppressWarnings("unchecked")
	@Test(timeout=10000)
	public void testAtLeastOnceProducer() throws Throwable {
		final DummyFlinkKafkaProducer<String> producer = new DummyFlinkKafkaProducer<>(
			FakeStandardProducerConfig.get(), null);

		// enable flushing
		producer.setFlushOnCheckpoint(true);

		final KafkaProducer<?, ?> mockProducer = producer.getMockKafkaProducer();

		final OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(producer));

		testHarness.open();

		testHarness.processElement(new StreamRecord<>("msg-1"));
		testHarness.processElement(new StreamRecord<>("msg-2"));
		testHarness.processElement(new StreamRecord<>("msg-3"));

		verify(mockProducer, times(3)).send(any(ProducerRecord.class), any(Callback.class));
		Assert.assertEquals(3, producer.getPendingSize());

		// start a thread to perform checkpointing
		final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
		Thread snapshotThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					// this should block until all records are flushed;
					// if the snapshot implementation returns before pending records are flushed,
					testHarness.snapshot(123L, 123L);
				} catch (Exception e) {
					exceptionRef.compareAndSet(null, e);
				}
			}
		});
		snapshotThread.start();

		// before proceeding, make sure that flushing has started and that the snapshot is still blocked;
		// this would block forever if the snapshot didn't perform a flush
		producer.waitUntilFlushStarted();
		Assert.assertTrue("Snapshot returned before all records were flushed", snapshotThread.isAlive());

		// now, complete the callbacks
		producer.getPendingCallbacks().get(0).onCompletion(null, null);
		Assert.assertTrue("Snapshot returned before all records were flushed", snapshotThread.isAlive());
		Assert.assertEquals(2, producer.getPendingSize());

		producer.getPendingCallbacks().get(1).onCompletion(null, null);
		Assert.assertTrue("Snapshot returned before all records were flushed", snapshotThread.isAlive());
		Assert.assertEquals(1, producer.getPendingSize());

		producer.getPendingCallbacks().get(2).onCompletion(null, null);
		Assert.assertEquals(0, producer.getPendingSize());

		snapshotThread.join();

		// snapshot would fail with an exception if flushing wasn't completed before the snapshot method returned;
		// make sure this did not happen
		assertTrue(exceptionRef.get() == null);

		testHarness.close();
	}

	/**
	 * This test is meant to assure that testAtLeastOnceProducer is valid by testing that if flushing is disabled,
	 * the snapshot method does indeed finishes without waiting for pending records;
	 * we set a timeout because the test will not finish if the logic is broken
	 */
	@SuppressWarnings("unchecked")
	@Test(timeout=5000)
	public void testDoesNotWaitForPendingRecordsIfFlushingDisabled() throws Throwable {
		final DummyFlinkKafkaProducer<String> producer = new DummyFlinkKafkaProducer<>(
			FakeStandardProducerConfig.get(), null);
		producer.setFlushOnCheckpoint(false);

		final KafkaProducer<?, ?> mockProducer = producer.getMockKafkaProducer();

		final OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(producer));

		testHarness.open();

		testHarness.processElement(new StreamRecord<>("msg"));

		// make sure that all callbacks have not been completed
		verify(mockProducer, times(1)).send(any(ProducerRecord.class), any(Callback.class));

		// should return even if there are pending records
		testHarness.snapshot(123L, 123L);

		testHarness.close();
	}

	// ------------------------------------------------------------------------

	private static class DummyFlinkKafkaProducer<T> extends FlinkKafkaProducerBase<T> {
		private static final long serialVersionUID = 1L;

		private final static String DUMMY_TOPIC = "dummy-topic";

		private transient KafkaProducer<?, ?> mockProducer;
		private transient List<Callback> pendingCallbacks;
		private transient MultiShotLatch flushLatch;
		private boolean isFlushed;

		@SuppressWarnings("unchecked")
		DummyFlinkKafkaProducer(Properties producerConfig, KafkaPartitioner partitioner) {

			super(DUMMY_TOPIC, (KeyedSerializationSchema<T>) mock(KeyedSerializationSchema.class), producerConfig, partitioner);

			this.pendingCallbacks = new ArrayList<>();
			this.flushLatch = new MultiShotLatch();
		}

		long getPendingSize() {
			if (flushOnCheckpoint) {
				return numPendingRecords();
			} else {
				// when flushing is disabled, the implementation does not
				// maintain the current number of pending records to reduce
				// the extra locking overhead required to do so
				throw new UnsupportedOperationException("getPendingSize not supported when flushing is disabled");
			}
		}

		List<Callback> getPendingCallbacks() {
			return pendingCallbacks;
		}

		KafkaProducer<?, ?> getMockKafkaProducer() {
			return mockProducer;
		}

		@Override
		public Serializable snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			isFlushed = false;

			Serializable snapshot = super.snapshotState(checkpointId, checkpointTimestamp);

			// if the snapshot implementation doesn't wait until all pending records are flushed, we should fail the test
			if (flushOnCheckpoint && !isFlushed) {
				throw new RuntimeException("Flushing is enabled; snapshots should be blocked until all pending records are flushed");
			}

			return snapshot;
		}

		public void waitUntilFlushStarted() throws Exception {
			flushLatch.await();
		}

		@SuppressWarnings("unchecked")
		@Override
		protected <K, V> KafkaProducer<K, V> getKafkaProducer(Properties props) {
			if (this.mockProducer == null) {
				this.mockProducer = mock(KafkaProducer.class);
				when(mockProducer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(new Answer<Object>() {
					@Override
					public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
						pendingCallbacks.add((Callback) invocationOnMock.getArguments()[1]);
						return null;
					}
				});
			}

			return (KafkaProducer<K, V>) this.mockProducer;
		}

		@Override
		protected void flush() {
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
		public RuntimeContext getRuntimeContext() {
			StreamingRuntimeContext runtimeContext = mock(StreamingRuntimeContext.class);
			when(runtimeContext.isCheckpointingEnabled()).thenReturn(true);
			when(runtimeContext.getMetricGroup()).thenReturn(mock(MetricGroup.class));
			return runtimeContext;
		}
	}
}
