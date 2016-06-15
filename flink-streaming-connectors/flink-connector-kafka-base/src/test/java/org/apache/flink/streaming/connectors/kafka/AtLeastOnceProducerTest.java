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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Assert;
import org.junit.Test;

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

	@Test
	public void testAtLeastOnceProducer() throws Exception {
		runTest(true);
	}

	// This test ensures that the actual test fails if the flushing is disabled
	@Test(expected = AssertionError.class)
	public void ensureTestFails() throws Exception {
		runTest(false);
	}

	private void runTest(boolean flushOnCheckpoint) throws Exception {
		Properties props = new Properties();
		final TestingKafkaProducer<String> producer = new TestingKafkaProducer<>("someTopic", new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), props);
		producer.setFlushOnCheckpoint(flushOnCheckpoint);
		producer.setRuntimeContext(new MockRuntimeContext(0, 1));

		producer.open(new Configuration());

		for(int i = 0; i < 100; i++) {
			producer.invoke("msg-" + i);
		}
		// start a thread confirming all pending records
		final Tuple1<Throwable> runnableError = new Tuple1<>(null);
		final Thread threadA = Thread.currentThread();
		final AtomicBoolean markOne = new AtomicBoolean(false);
		Runnable confirmer = new Runnable() {
			@Override
			public void run() {
				try {
					MockProducer mp = producer.getProducerInstance();
					List<Callback> pending = mp.getPending();

					// we ensure thread A is locked and didn't reach markOne
					// give thread A some time to really reach the snapshot state
					Thread.sleep(500);
					if(markOne.get()) {
						Assert.fail("Snapshot was confirmed even though messages " +
								"were still in the buffer");
					}
					Assert.assertEquals(100, pending.size());

					// now confirm all checkpoints
					for(Callback c: pending) {
						c.onCompletion(null, null);
					}
					pending.clear();
					// wait for the snapshotState() method to return
					Thread.sleep(100);
					Assert.assertTrue("Snapshot state didn't return", markOne.get());
				} catch(Throwable t) {
					runnableError.f0 = t;
				}
			}
		};
		Thread threadB = new Thread(confirmer);
		threadB.start();
		// this should block:
		producer.snapshotState(0, 0);
		// once all pending callbacks are confirmed, we can set this marker to true
		markOne.set(true);
		for(int i = 0; i < 99; i++) {
			producer.invoke("msg-" + i);
		}
		// wait at most one second
		threadB.join(800L);
		Assert.assertFalse("Thread A reached this point too fast", threadB.isAlive());
		if(runnableError.f0 != null) {
			runnableError.f0.printStackTrace();
			Assert.fail("Error from thread B: " + runnableError.f0 );
		}

		producer.close();
	}


	private static class TestingKafkaProducer<T> extends FlinkKafkaProducerBase<T> {
		private MockProducer prod;

		public TestingKafkaProducer(String defaultTopicId, KeyedSerializationSchema<T> serializationSchema, Properties producerConfig) {
			super(defaultTopicId, serializationSchema, producerConfig, null);
		}

		@Override
		protected <K, V> Producer<K, V> getKafkaProducer(Properties props) {
			this.prod = new MockProducer();
			return this.prod;
		}

		public MockProducer getProducerInstance() {
			return this.prod;
		}
	}

	private static class MockProducer<K, V> implements Producer<K, V> {
		List<Callback> pendingCallbacks = new ArrayList<>();

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

		@Override
		public void close() {

		}

		public List<Callback> getPending() {
			return this.pendingCallbacks;
		}
	}
}
