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

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.testutils.MockRuntimeContext;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Assert;
import org.junit.Test;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test ensuring that the producer is not dropping buffered records
 */
@SuppressWarnings("unchecked")
public class AtLeastOnceProducerTest {

	// we set a timeout because the test will not finish if the logic is broken
	@Test(timeout=5000)
	public void testAtLeastOnceProducer() throws Throwable {
		runTest(true);
	}

	// This test ensures that the actual test fails if the flushing is disabled
	@Test(expected = AssertionError.class, timeout=5000)
	public void ensureTestFails() throws Throwable {
		runTest(false);
	}

	private void runTest(boolean flushOnCheckpoint) throws Throwable {
		Properties props = new Properties();
		final AtomicBoolean snapshottingFinished = new AtomicBoolean(false);
		final TestingKafkaProducer<String> producer = new TestingKafkaProducer<>("someTopic", new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), props,
				snapshottingFinished);
		producer.setFlushOnCheckpoint(flushOnCheckpoint);
		producer.setRuntimeContext(new MockRuntimeContext(0, 1));

		producer.open(new Configuration());

		for (int i = 0; i < 100; i++) {
			producer.invoke("msg-" + i);
		}
		// start a thread confirming all pending records
		final Tuple1<Throwable> runnableError = new Tuple1<>(null);
		final Thread threadA = Thread.currentThread();

		Runnable confirmer = new Runnable() {
			@Override
			public void run() {
				try {
					MockProducer mp = producer.getProducerInstance();
					List<Callback> pending = mp.getPending();

					// we need to find out if the snapshot() method blocks forever
					// this is not possible. If snapshot() is running, it will
					// start removing elements from the pending list.
					synchronized (threadA) {
						threadA.wait(500L);
					}
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
		Thread threadB = new Thread(confirmer);
		threadB.start();
		// this should block:
		producer.snapshotState(0, 0);
		synchronized (threadA) {
			threadA.notifyAll(); // just in case, to let the test fail faster
		}
		Assert.assertEquals(0, producer.getProducerInstance().getPending().size());
		Deadline deadline = FiniteDuration.apply(5, "s").fromNow();
		while (deadline.hasTimeLeft() && threadB.isAlive()) {
			threadB.join(500);
		}
		Assert.assertFalse("Thread A is expected to be finished at this point. If not, the test is prone to fail", threadB.isAlive());
		if (runnableError.f0 != null) {
			throw runnableError.f0;
		}

		producer.close();
	}


	private static class TestingKafkaProducer<T> extends FlinkKafkaProducerBase<T> {
		private MockProducer prod;
		private AtomicBoolean snapshottingFinished;

		public TestingKafkaProducer(String defaultTopicId, KeyedSerializationSchema<T> serializationSchema, Properties producerConfig, AtomicBoolean snapshottingFinished) {
			super(defaultTopicId, serializationSchema, producerConfig, null);
			this.snapshottingFinished = snapshottingFinished;
		}

		@Override
		protected <K, V> KafkaProducer<K, V> getKafkaProducer(Properties props) {
			this.prod = new MockProducer();
			return this.prod;
		}

		@Override
		public Serializable snapshotState(long checkpointId, long checkpointTimestamp) {
			// call the actual snapshot state
			Serializable ret = super.snapshotState(checkpointId, checkpointTimestamp);
			// notify test that snapshotting has been done
			snapshottingFinished.set(true);
			return ret;
		}

		@Override
		protected void flush() {
			this.prod.flush();
		}

		public MockProducer getProducerInstance() {
			return this.prod;
		}
	}

	private static class MockProducer<K, V> extends KafkaProducer<K, V> {
		List<Callback> pendingCallbacks = new ArrayList<>();

		private static Properties getFakeProperties() {
			Properties p = new Properties();
			p.setProperty("bootstrap.servers", "localhost:12345");
			p.setProperty("key.serializer", ByteArraySerializer.class.getName());
			p.setProperty("value.serializer", ByteArraySerializer.class.getName());
			return p;
		}
		public MockProducer() {
			super(getFakeProperties());
		}

		@Override
		public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
			throw new UnsupportedOperationException("Unexpected");
		}

		@Override
		public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
			pendingCallbacks.add(callback);
			return null;
		}

		@Override
		public List<PartitionInfo> partitionsFor(String topic) {
			List<PartitionInfo> list = new ArrayList<>();
			list.add(new PartitionInfo(topic, 0, null, null, null));
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
