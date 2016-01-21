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

package org.apache.flink.streaming.connectors.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.SerializedCheckpointData;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.modules.junit4.PowerMockRunner;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Tests for the RMQSource. The source supports two operation modes.
 * 1) Exactly-once (when checkpointed) with RabbitMQ transactions and the deduplication mechanism in
 *    {@link org.apache.flink.streaming.api.functions.source.MessageAcknowledgingSourceBase}.
 * 2) At-least-once (when checkpointed) with RabbitMQ transactions but not deduplication.
 * 3) No strong delivery guarantees (without checkpointing) with RabbitMQ auto-commit mode.
 *
 * This tests assumes that the message ids are increasing monotonously. That doesn't have to be the
 * case. The correlation id is used to uniquely identify messages.
 */
@RunWith(PowerMockRunner.class)
public class RMQSourceTest {

	private RMQSource<String> source;

	private Configuration config = new Configuration();

	private Thread sourceThread;

	private volatile long messageId;

	private boolean generateCorrelationIds = true;

	private volatile Exception exception;

	@Before
	public void beforeTest() throws Exception {

		source = new RMQTestSource<>("hostDummy", "queueDummy", true, new StringDeserializationScheme());
		source.open(config);
		source.initializeConnection();

		messageId = 0;

		sourceThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					source.run(new DummySourceContext());
				} catch (Exception e) {
					exception = e;
				}
			}
		});
	}

	@After
	public void afterTest() throws Exception {
		source.cancel();
		sourceThread.join();
	}

	/**
	 * Make sure concurrent access to snapshotState() and notifyCheckpointComplete() don't cause
	 * an issue.
	 *
	 * Without proper synchronization, the test will fail with a concurrent modification exception
	 *
	 */
	@Test
	public void testConcurrentAccess() throws Exception {
		source.autoAck = false;
		sourceThread.start();

		final Tuple1<Throwable> error = new Tuple1<>(null);

		Thread.sleep(5);

		Thread snapshotThread = new Thread(new Runnable() {
			public long id = 0;

			@Override
			public void run() {
				while (!Thread.interrupted()) {
					try {
						source.snapshotState(id++, 0);
					} catch (Exception e) {
						error.f0 = e;
						break; // stop thread
					}
				}
			}
		});

		Thread notifyThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (!Thread.interrupted()) {
					try {
						// always remove all checkpoints
						source.notifyCheckpointComplete(Long.MAX_VALUE);
					} catch (Exception e) {
						error.f0 = e;
						break; // stop thread
					}
				}
			}
		});

		snapshotThread.start();
		notifyThread.start();

		long deadline = System.currentTimeMillis() + 1000L;
		while(System.currentTimeMillis() < deadline) {
			if(!snapshotThread.isAlive()) {
				notifyThread.interrupt();
				break;
			}
			if(!notifyThread.isAlive()) {
				snapshotThread.interrupt();
				break;
			}
			Thread.sleep(10);
		}
		if(snapshotThread.isAlive()) {
			snapshotThread.interrupt();
			snapshotThread.join();
		}
		if(notifyThread.isAlive()) {
			notifyThread.interrupt();
			notifyThread.join();
		}
		if(error.f0 != null) {
			error.f0.printStackTrace();
			Assert.fail("Test failed with " + error.f0.getClass().getCanonicalName());
		}

	}

	@Test
	public void testCheckpointing() throws Exception {
		source.autoAck = false;
		sourceThread.start();

		Thread.sleep(5);

		final Random random = new Random(System.currentTimeMillis());
		int numSnapshots = 50;
		long previousSnapshotId;
		long lastSnapshotId = 0;

		long totalNumberOfAcks = 0;

		for (int i=0; i < numSnapshots; i++) {
			long snapshotId = random.nextLong();
			SerializedCheckpointData[] data;

			synchronized (DummySourceContext.lock) {
				data = source.snapshotState(snapshotId, System.currentTimeMillis());
				previousSnapshotId = lastSnapshotId;
				lastSnapshotId = messageId;
			}
			// let some time pass
			Thread.sleep(5);

			// check if the correct number of messages have been snapshotted
			final long numIds = lastSnapshotId - previousSnapshotId;
			assertEquals(numIds, data[0].getNumIds());
			// deserialize and check if the last id equals the last snapshotted id
			ArrayDeque<Tuple2<Long, List<String>>> deque = SerializedCheckpointData.toDeque(data, new StringSerializer());
			List<String> messageIds = deque.getLast().f1;
			if (messageIds.size() > 0) {
				assertEquals(lastSnapshotId, (long) Long.valueOf(messageIds.get(messageIds.size() - 1)));
			}

			// check if the messages are being acknowledged and the transaction comitted
			source.notifyCheckpointComplete(snapshotId);
			totalNumberOfAcks += numIds;

		}

		Mockito.verify(source.channel, Mockito.times((int) totalNumberOfAcks)).basicAck(Mockito.anyLong(), Mockito.eq(false));
		Mockito.verify(source.channel, Mockito.times(numSnapshots)).txCommit();

	}

	/**
	 * Checks whether recurring ids are processed again (they shouldn't be).
	 */
	@Test
	public void testDuplicateId() throws Exception {
		source.autoAck = false;
		sourceThread.start();

		while (messageId < 10) {
			// wait until messages have been processed
			Thread.sleep(5);
		}

		long oldMessageId;
		synchronized (DummySourceContext.lock) {
			oldMessageId = messageId;
			messageId = 0;
		}

		while (messageId < 10) {
			// process again
			Thread.sleep(5);
		}

		synchronized (DummySourceContext.lock) {
			assertEquals(Math.max(messageId, oldMessageId), DummySourceContext.numElementsCollected);
		}
	}


	/**
	 * The source should not acknowledge ids in auto-commit mode or check for previously acknowledged ids
	 */
	@Test
	public void testCheckpointingDisabled() throws Exception {
		source.autoAck = true;
		sourceThread.start();

		while (DummySourceContext.numElementsCollected < 50) {
			// wait until messages have been processed
			Thread.sleep(5);
		}

		// see addId in RMQTestSource.addId for the assert
	}

	/**
	 * Tests error reporting in case of invalid correlation ids
	 */
	@Test
	public void testCorrelationIdNotSet() throws InterruptedException {
		generateCorrelationIds = false;
		source.autoAck = false;
		sourceThread.start();

		sourceThread.join();

		assertNotNull(exception);
		assertTrue(exception instanceof NullPointerException);
	}


	private static class StringDeserializationScheme implements DeserializationSchema<String> {

		@Override
		public String deserialize(byte[] message) throws IOException {
			try {
				// wait a bit to not cause too much cpu load
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return new String(message);
		}

		@Override
		public boolean isEndOfStream(String nextElement) {
			return false;
		}

		@Override
		public TypeInformation<String> getProducedType() {
			return TypeExtractor.getForClass(String.class);
		}
	}

	private class RMQTestSource<OUT> extends RMQSource<OUT> {

		public RMQTestSource(String hostName, String queueName, boolean usesCorrelationIds,
							 DeserializationSchema<OUT> deserializationSchema) {
			super(hostName, queueName, usesCorrelationIds, deserializationSchema);
		}

		@Override
		protected void initializeConnection() {
			connection = Mockito.mock(Connection.class);
			channel = Mockito.mock(Channel.class);
			consumer = Mockito.mock(QueueingConsumer.class);

			// Mock for delivery
			final QueueingConsumer.Delivery deliveryMock = Mockito.mock(QueueingConsumer.Delivery.class);
			Mockito.when(deliveryMock.getBody()).thenReturn("test".getBytes());

			// Mock for envelope
			Envelope envelope = Mockito.mock(Envelope.class);
			Mockito.when(deliveryMock.getEnvelope()).thenReturn(envelope);

			try {
				Mockito.when(consumer.nextDelivery()).thenReturn(deliveryMock);
			} catch (InterruptedException e) {
				fail("Couldn't setup up deliveryMock");
			}

			Mockito.when(envelope.getDeliveryTag()).thenAnswer(new Answer<Long>() {
				@Override
				public Long answer(InvocationOnMock invocation) throws Throwable {
					return ++messageId;
				}
			});

			// Mock for properties
			AMQP.BasicProperties props = Mockito.mock(AMQP.BasicProperties.class);
			Mockito.when(deliveryMock.getProperties()).thenReturn(props);

			Mockito.when(props.getCorrelationId()).thenAnswer(new Answer<String>() {
				@Override
				public String answer(InvocationOnMock invocation) throws Throwable {
					return generateCorrelationIds ? "" + messageId : null;
				}
			});

		}

		@Override
		protected boolean addId(String uid) {
			assertEquals(false, autoAck);
			return super.addId(uid);
		}
	}

	private static class DummySourceContext implements SourceFunction.SourceContext<String> {

		private static final Object lock = new Object();

		private static long numElementsCollected;

		public DummySourceContext() {
			numElementsCollected = 0;
		}

		@Override
		public void collect(String element) {
			numElementsCollected++;
		}

		@Override
		public void collectWithTimestamp(java.lang.String element, long timestamp) {
		}

		@Override
		public void emitWatermark(Watermark mark) {
		}

		@Override
		public Object getCheckpointLock() {
			return lock;
		}

		@Override
		public void close() {
		}
	}
}
