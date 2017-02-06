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
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.testutils.FakeStandardProducerConfig;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlinkKafkaProducerBaseTest {

	/**
	 * Tests that the constructor eagerly checks bootstrap servers are set in config
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testInstantiationFailsWhenBootstrapServersMissing() throws Exception {
		// no bootstrap servers set in props
		Properties props = new Properties();
		// should throw IllegalArgumentException
		new DummyFlinkKafkaProducer<>(props, null);
	}

	/**
	 * Tests that constructor defaults to key value serializers in config to byte array deserializers if not set
	 */
	@Test
	public void testKeyValueDeserializersSetIfMissing() throws Exception {
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:12345");
		// should set missing key value deserializers
		new DummyFlinkKafkaProducer<>(props, null);

		assertTrue(props.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
		assertTrue(props.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
		assertTrue(props.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).equals(ByteArraySerializer.class.getCanonicalName()));
		assertTrue(props.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).equals(ByteArraySerializer.class.getCanonicalName()));
	}

	/**
	 * Tests that partitions list is determinate and correctly provided to custom partitioner
	 */
	@Test
	public void testPartitionerOpenedWithDeterminatePartitionList() throws Exception {
		KafkaPartitioner mockPartitioner = mock(KafkaPartitioner.class);
		RuntimeContext mockRuntimeContext = mock(RuntimeContext.class);
		when(mockRuntimeContext.getIndexOfThisSubtask()).thenReturn(0);
		when(mockRuntimeContext.getNumberOfParallelSubtasks()).thenReturn(1);

		DummyFlinkKafkaProducer producer = new DummyFlinkKafkaProducer(
			FakeStandardProducerConfig.get(), mockPartitioner);
		producer.setRuntimeContext(mockRuntimeContext);

		producer.open(new Configuration());

		// the internal mock KafkaProducer will return an out-of-order list of 4 partitions,
		// which should be sorted before provided to the custom partitioner's open() method
		int[] correctPartitionList = {0, 1, 2, 3};
		verify(mockPartitioner).open(0, 1, correctPartitionList);
	}


	/**
	 * Test ensuring that if an invoke call happens right after an async exception is caught, it should be rethrown
	 */
	@Test
	public void testAsyncErrorRethrownOnInvoke() throws Throwable {
		final OneShotLatch inputLatch = new OneShotLatch();

		final DummyFlinkKafkaProducer<String> producer =
			new DummyFlinkKafkaProducer<>(FakeStandardProducerConfig.get(), null, inputLatch, 1, new AtomicBoolean(true));

		OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink(producer));

		testHarness.open();

		List<Callback> pending = producer.getProducerInstance().getPending();

		testHarness.processElement(new StreamRecord<>("msg-1"));

		inputLatch.await();

		// let the message request return an async exception
		pending.get(0).onCompletion(null, new Exception("artificial async exception"));

		try {
			testHarness.processElement(new StreamRecord<>("msg-2"));
		} catch (Exception e) {
			// the next invoke should rethrow the async exception
			Assert.assertTrue(e.getCause().getMessage().contains("artificial async exception"));
			return;
		}

		Assert.fail();
	}

	/**
	 * Test ensuring that if a snapshot call happens right after an async exception is caught, it should be rethrown
	 */
	@Test
	public void testAsyncErrorRethrownOnCheckpoint() throws Throwable {
		final OneShotLatch inputLatch = new OneShotLatch();

		final DummyFlinkKafkaProducer<String> producer =
			new DummyFlinkKafkaProducer<>(FakeStandardProducerConfig.get(), null, inputLatch, 1, new AtomicBoolean(true));

		OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink(producer));

		testHarness.open();

		List<Callback> pending = producer.getProducerInstance().getPending();

		testHarness.processElement(new StreamRecord<>("msg-1"));

		inputLatch.await();

		// let the message request return an async exception
		pending.get(0).onCompletion(null, new Exception("artificial async exception"));

		try {
			testHarness.snapshot(123L, 123L);
		} catch (Exception e) {
			// the next invoke should rethrow the async exception
			Assert.assertTrue(e.getCause().getMessage().contains("artificial async exception"));
			return;
		}

		Assert.fail();
	}

	/**
	 * Test ensuring that if an async exception is caught for one of the flushed requests on checkpoint,
	 * it should be rethrown; we set a timeout because the test will not finish if the logic is broken.
	 */
	@Test(timeout=500)
	public void testAsyncErrorRethrownOnCheckpointAfterFlush() throws Throwable {
		final OneShotLatch inputLatch = new OneShotLatch();

		final DummyFlinkKafkaProducer<String> producer =
			new DummyFlinkKafkaProducer<>(FakeStandardProducerConfig.get(), null, inputLatch, 3, new AtomicBoolean(true));
		producer.setFlushOnCheckpoint(true);

		final OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink(producer));

		testHarness.open();

		List<Callback> pending = producer.getProducerInstance().getPending();

		testHarness.processElement(new StreamRecord<>("msg-1"));
		testHarness.processElement(new StreamRecord<>("msg-2"));
		testHarness.processElement(new StreamRecord<>("msg-3"));
		inputLatch.await();

		// only let the first callback succeed for now
		pending.get(0).onCompletion(null, null);

		final Tuple1<Throwable> asyncError = new Tuple1<>(null);
		Thread snapshotThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					// this should block at first, since there are still two pending records that needs to be flushed
					testHarness.snapshot(123L, 123L);
				} catch (Exception e) {
					asyncError.f0 = e;
				}
			}
		});
		snapshotThread.start();

		// let the 2nd message fail with an async exception
		pending.get(1).onCompletion(null, new Exception("artificial async failure for 2nd message"));
		pending.get(2).onCompletion(null, null);

		snapshotThread.join();

		// the snapshot should have failed with the async exception
		Assert.assertTrue(asyncError.f0 != null && asyncError.f0.getCause().getMessage().contains("artificial async failure for 2nd message"));
	}

	/**
	 * Test ensuring that the producer is not dropping buffered records;
	 * we set a timeout because the test will not finish if the logic is broken
	 */
	@Test(timeout=5000)
	public void testAtLeastOnceProducer() throws Throwable {
		final OneShotLatch inputLatch = new OneShotLatch();

		final AtomicBoolean snapshottingFinished = new AtomicBoolean(false);
		final DummyFlinkKafkaProducer<String> producer = new DummyFlinkKafkaProducer<>(
			FakeStandardProducerConfig.get(), null, inputLatch, 100, snapshottingFinished);
		producer.setFlushOnCheckpoint(true);

		OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink(producer));

		testHarness.open();

		for (int i = 0; i < 100; i++) {
			testHarness.processElement(new StreamRecord<>("msg-" + i));
		}

		// start a thread confirming all pending records
		final Tuple1<Throwable> runnableError = new Tuple1<>(null);

		Runnable confirmer = new Runnable() {
			@Override
			public void run() {
				try {
					MockProducer mp = producer.getProducerInstance();
					List<Callback> pending = mp.getPending();

					// wait until all records have been added to producer
					inputLatch.await();

					// we now check that no records have been confirmed yet
					Assert.assertEquals(100, pending.size());

					Assert.assertFalse("Snapshot method returned before all records were confirmed",
						snapshottingFinished.get());

					// now confirm all checkpoints
					for (Callback c: pending) {
						c.onCompletion(null, null);
					}
					pending.clear();
				} catch(Throwable t) {
					runnableError.f0 = t;
				}
			}
		};
		Thread callbackThread = new Thread(confirmer);
		callbackThread.start();

		// this will block if flushOnCheckpoint is enabled
		// it should not block forever, because the callback thread is confirming completion for all records
		testHarness.snapshot(0, 0);

		// since flushOnCheckpoint is enabled, the snapshot method should block until there are no more pending records
		Assert.assertEquals(0, producer.getProducerInstance().getPending().size());

		if (runnableError.f0 != null) {
			throw runnableError.f0;
		}

		testHarness.close();
	}

	// ------------------------------------------------------------------------

	private static class DummyFlinkKafkaProducer<T> extends FlinkKafkaProducerBase<T> {
		private static final long serialVersionUID = 1L;

		private final OneShotLatch inputLatch;
		private final int numInputRecordsTriggerLatch;

		private transient MockProducer prod;
		private AtomicBoolean snapshottingFinished;

		@SuppressWarnings("unchecked")
		public DummyFlinkKafkaProducer(
				Properties producerConfig,
				KafkaPartitioner partitioner,
				OneShotLatch inputLatch,
				int numInputRecordsTriggerLatch,
				AtomicBoolean snapshottingFinished) {
			super("dummy-topic", (KeyedSerializationSchema< T >) mock(KeyedSerializationSchema.class), producerConfig, partitioner);
			this.snapshottingFinished = snapshottingFinished;
			this.inputLatch = inputLatch;
			this.numInputRecordsTriggerLatch = numInputRecordsTriggerLatch;
		}

		// constructor variant for test irrelated to snapshotting
		@SuppressWarnings("unchecked")
		public DummyFlinkKafkaProducer(
				Properties producerConfig,
				KafkaPartitioner partitioner) {
			this(producerConfig, partitioner, new OneShotLatch(), 10, new AtomicBoolean(true));
		}

		@Override
		protected <K, V> KafkaProducer<K, V> getKafkaProducer(Properties props) {
			this.prod = new MockProducer(inputLatch, numInputRecordsTriggerLatch);
			return this.prod;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
			// call the actual snapshot state
			super.snapshotState(ctx);
			// notify test that snapshotting has been done
			snapshottingFinished.set(true);
		}

		@Override
		protected void flush() {
			this.prod.flush();
		}

		public MockProducer getProducerInstance() {
			return this.prod;
		}
	}

	/** A mock Kafka producer that accepts a one-shot latch to trigger once an initial number of records are processed. */
	private static class MockProducer<K, V> extends KafkaProducer<K, V> {
		List<Callback> pendingCallbacks = new ArrayList<>();

		private final OneShotLatch inputLatch;
		private final int numInputRecordsTriggerLatch;

		public MockProducer(OneShotLatch inputLatch, int numInputRecordsTriggerLatch) {
			super(FakeStandardProducerConfig.get());
			this.inputLatch = inputLatch;
			this.numInputRecordsTriggerLatch = numInputRecordsTriggerLatch;
		}

		@Override
		public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
			throw new UnsupportedOperationException("Unexpected");
		}

		@Override
		public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
			pendingCallbacks.add(callback);

			if (pendingCallbacks.size() == numInputRecordsTriggerLatch) {
				inputLatch.trigger();
			}

			return null;
		}

		@Override
		public List<PartitionInfo> partitionsFor(String topic) {
			List<PartitionInfo> list = new ArrayList<>();
			// deliberately return an out-of-order partition list
			list.add(new PartitionInfo(topic, 3, null, null, null));
			list.add(new PartitionInfo(topic, 1, null, null, null));
			list.add(new PartitionInfo(topic, 0, null, null, null));
			list.add(new PartitionInfo(topic, 2, null, null, null));
			return list;
		}

		@Override
		public Map<MetricName, ? extends Metric> metrics() {
			return null;
		}


		public List<Callback> getPending() {
			return this.pendingCallbacks;
		}

		public void flush() {
			while (pendingCallbacks.size() > 0) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					throw new RuntimeException("Unable to flush producer, task was interrupted");
				}
			}
		}
	}
}
