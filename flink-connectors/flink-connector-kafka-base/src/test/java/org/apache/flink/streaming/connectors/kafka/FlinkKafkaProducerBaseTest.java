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
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.SinkContextUtil;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.testutils.FakeStandardProducerConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link FlinkKafkaProducerBase}.
 */
public class FlinkKafkaProducerBaseTest {

	/**
	 * Tests that the constructor eagerly checks bootstrap servers are set in config.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testInstantiationFailsWhenBootstrapServersMissing() throws Exception {
		// no bootstrap servers set in props
		Properties props = new Properties();
		// should throw IllegalArgumentException
		new DummyFlinkKafkaProducer<>(props, new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), null);
	}

	/**
	 * Tests that constructor defaults to key value serializers in config to byte array deserializers if not set.
	 */
	@Test
	public void testKeyValueDeserializersSetIfMissing() throws Exception {
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:12345");
		// should set missing key value deserializers
		new DummyFlinkKafkaProducer<>(props, new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), null);

		assertTrue(props.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
		assertTrue(props.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
		assertTrue(props.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).equals(ByteArraySerializer.class.getName()));
		assertTrue(props.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).equals(ByteArraySerializer.class.getName()));
	}

	/**
	 * Tests that partitions list is determinate and correctly provided to custom partitioner.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testPartitionerInvokedWithDeterminatePartitionList() throws Exception {
		FlinkKafkaPartitioner<String> mockPartitioner = mock(FlinkKafkaPartitioner.class);

		RuntimeContext mockRuntimeContext = mock(StreamingRuntimeContext.class);
		when(mockRuntimeContext.getIndexOfThisSubtask()).thenReturn(0);
		when(mockRuntimeContext.getNumberOfParallelSubtasks()).thenReturn(1);

		// out-of-order list of 4 partitions
		List<PartitionInfo> mockPartitionsList = new ArrayList<>(4);
		mockPartitionsList.add(new PartitionInfo(DummyFlinkKafkaProducer.DUMMY_TOPIC, 3, null, null, null));
		mockPartitionsList.add(new PartitionInfo(DummyFlinkKafkaProducer.DUMMY_TOPIC, 1, null, null, null));
		mockPartitionsList.add(new PartitionInfo(DummyFlinkKafkaProducer.DUMMY_TOPIC, 0, null, null, null));
		mockPartitionsList.add(new PartitionInfo(DummyFlinkKafkaProducer.DUMMY_TOPIC, 2, null, null, null));

		final DummyFlinkKafkaProducer<String> producer = new DummyFlinkKafkaProducer<>(
			FakeStandardProducerConfig.get(), new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), mockPartitioner);
		producer.setRuntimeContext(mockRuntimeContext);

		final KafkaProducer mockProducer = producer.getMockKafkaProducer();
		when(mockProducer.partitionsFor(anyString())).thenReturn(mockPartitionsList);
		when(mockProducer.metrics()).thenReturn(null);

		producer.open(new Configuration());
		verify(mockPartitioner, times(1)).open(0, 1);

		producer.invoke("foobar", SinkContextUtil.forTimestamp(0));
		verify(mockPartitioner, times(1)).partition(
			"foobar", null, "foobar".getBytes(), DummyFlinkKafkaProducer.DUMMY_TOPIC, new int[] {0, 1, 2, 3});
	}

	/**
	 * Test ensuring that if an invoke call happens right after an async exception is caught, it should be rethrown.
	 */
	@Test
	public void testAsyncErrorRethrownOnInvoke() throws Throwable {
		final DummyFlinkKafkaProducer<String> producer = new DummyFlinkKafkaProducer<>(
			FakeStandardProducerConfig.get(), new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), null);

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
	 * Test ensuring that if a snapshot call happens right after an async exception is caught, it should be rethrown.
	 */
	@Test
	public void testAsyncErrorRethrownOnCheckpoint() throws Throwable {
		final DummyFlinkKafkaProducer<String> producer = new DummyFlinkKafkaProducer<>(
			FakeStandardProducerConfig.get(), new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), null);

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
	 * <p>Note that this test does not test the snapshot method is blocked correctly when there are pending records.
	 * The test for that is covered in testAtLeastOnceProducer.
	 */
	@SuppressWarnings("unchecked")
	@Test(timeout = 5000)
	public void testAsyncErrorRethrownOnCheckpointAfterFlush() throws Throwable {
		final DummyFlinkKafkaProducer<String> producer = new DummyFlinkKafkaProducer<>(
			FakeStandardProducerConfig.get(), new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), null);
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

		CheckedThread snapshotThread = new CheckedThread() {
			@Override
			public void go() throws Exception {
				// this should block at first, since there are still two pending records that needs to be flushed
				testHarness.snapshot(123L, 123L);
			}
		};
		snapshotThread.start();

		// let the 2nd message fail with an async exception
		producer.getPendingCallbacks().get(1).onCompletion(null, new Exception("artificial async failure for 2nd message"));
		producer.getPendingCallbacks().get(2).onCompletion(null, null);

		try {
			snapshotThread.sync();
		} catch (Exception e) {
			// the snapshot should have failed with the async exception
			Assert.assertTrue(e.getCause().getMessage().contains("artificial async failure for 2nd message"));

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
		final DummyFlinkKafkaProducer<String> producer = new DummyFlinkKafkaProducer<>(
			FakeStandardProducerConfig.get(), new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), null);
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
	@Test(timeout = 5000)
	public void testDoesNotWaitForPendingRecordsIfFlushingDisabled() throws Throwable {
		final DummyFlinkKafkaProducer<String> producer = new DummyFlinkKafkaProducer<>(
			FakeStandardProducerConfig.get(), new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), null);
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

		private static final String DUMMY_TOPIC = "dummy-topic";

		private transient KafkaProducer<?, ?> mockProducer;
		private transient List<Callback> pendingCallbacks;
		private transient MultiShotLatch flushLatch;
		private boolean isFlushed;

		@SuppressWarnings("unchecked")
		DummyFlinkKafkaProducer(Properties producerConfig, KeyedSerializationSchema<T> schema, FlinkKafkaPartitioner partitioner) {

			super(DUMMY_TOPIC, schema, producerConfig, partitioner);

			this.mockProducer = mock(KafkaProducer.class);
			when(mockProducer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
					pendingCallbacks.add(invocationOnMock.getArgumentAt(1, Callback.class));
					return null;
				}
			});

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
		public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
			isFlushed = false;

			super.snapshotState(ctx);

			// if the snapshot implementation doesn't wait until all pending records are flushed, we should fail the test
			if (flushOnCheckpoint && !isFlushed) {
				throw new RuntimeException("Flushing is enabled; snapshots should be blocked until all pending records are flushed");
			}
		}

		public void waitUntilFlushStarted() throws Exception {
			flushLatch.await();
		}

		@SuppressWarnings("unchecked")
		@Override
		protected <K, V> KafkaProducer<K, V> getKafkaProducer(Properties props) {
			return (KafkaProducer<K, V>) mockProducer;
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
	}
}
