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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.MockDeserializationSchema;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;

/**
 * Tests for the RMQSource. The source supports two operation modes.
 * 1) Exactly-once (when checkpointed) with RabbitMQ transactions and the deduplication mechanism in
 *    {@link org.apache.flink.streaming.api.functions.source.MessageAcknowledgingSourceBase}.
 * 2) At-least-once (when checkpointed) with RabbitMQ transactions but not deduplication.
 * 3) No strong delivery guarantees (without checkpointing) with RabbitMQ auto-commit mode.
 *
 * <p>This tests assumes that the message ids are increasing monotonously. That doesn't have to be the
 * case. The correlation id is used to uniquely identify messages.
 */
@RunWith(PowerMockRunner.class)
public class RMQSourceTest {

	private RMQSource<String> source;

	private Configuration config = new Configuration();

	private Thread sourceThread;

	private volatile long messageId;

	private boolean generateCorrelationIds;

	private volatile Exception exception;

	/**
	 * Gets a mock context for initializing the source's state via {@link org.apache.flink.streaming.api.checkpoint.CheckpointedFunction#initializeState}.
	 * @throws Exception
	 */
	FunctionInitializationContext getMockContext() throws Exception {
		OperatorStateStore mockStore = Mockito.mock(OperatorStateStore.class);
		FunctionInitializationContext mockContext = Mockito.mock(FunctionInitializationContext.class);
		Mockito.when(mockContext.getOperatorStateStore()).thenReturn(mockStore);
		Mockito.when(mockStore.getListState(any(ListStateDescriptor.class))).thenReturn(null);
		return mockContext;
	}

	@Before
	public void beforeTest() throws Exception {
		FunctionInitializationContext mockContext = getMockContext();

		source = new RMQTestSource();
		source.initializeState(mockContext);
		source.open(config);

		messageId = 0;
		generateCorrelationIds = true;

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

	@Test
	public void throwExceptionIfConnectionFactoryReturnNull() throws Exception {
		RMQConnectionConfig connectionConfig = Mockito.mock(RMQConnectionConfig.class);
		ConnectionFactory connectionFactory = Mockito.mock(ConnectionFactory.class);
		Connection connection = Mockito.mock(Connection.class);
		Mockito.when(connectionConfig.getConnectionFactory()).thenReturn(connectionFactory);
		Mockito.when(connectionFactory.newConnection()).thenReturn(connection);
		Mockito.when(connection.createChannel()).thenReturn(null);

		RMQSource<String> rmqSource = new RMQSource<>(
			connectionConfig, "queueDummy", true, new StringDeserializationScheme());
		try {
			rmqSource.open(new Configuration());
		} catch (RuntimeException ex) {
			assertEquals("None of RabbitMQ channels are available", ex.getMessage());
		}
	}

	@Test
	public void testOpenCallDeclaresQueueInStandardMode() throws Exception {
		FunctionInitializationContext mockContext = getMockContext();

		RMQConnectionConfig connectionConfig = Mockito.mock(RMQConnectionConfig.class);
		ConnectionFactory connectionFactory = Mockito.mock(ConnectionFactory.class);
		Connection connection = Mockito.mock(Connection.class);
		Channel channel = Mockito.mock(Channel.class);

		Mockito.when(connectionConfig.getConnectionFactory()).thenReturn(connectionFactory);
		Mockito.when(connectionFactory.newConnection()).thenReturn(connection);
		Mockito.when(connection.createChannel()).thenReturn(channel);

		RMQSource<String> rmqSource = new RMQMockedRuntimeTestSource(connectionConfig);
		rmqSource.initializeState(mockContext);
		rmqSource.open(new Configuration());

		Mockito.verify(channel).queueDeclare(RMQTestSource.QUEUE_NAME, true, false, false, null);
	}

	@Test
	public void testCheckpointing() throws Exception {
		source.autoAck = false;

		StreamSource<String, RMQSource<String>> src = new StreamSource<>(source);
		AbstractStreamOperatorTestHarness<String> testHarness =
			new AbstractStreamOperatorTestHarness<>(src, 1, 1, 0);
		testHarness.open();

		sourceThread.start();

		Thread.sleep(5);

		final Random random = new Random(System.currentTimeMillis());
		int numSnapshots = 50;
		long previousSnapshotId;
		long lastSnapshotId = 0;

		long totalNumberOfAcks = 0;

		for (int i = 0; i < numSnapshots; i++) {
			long snapshotId = random.nextLong();
			OperatorSubtaskState data;

			synchronized (DummySourceContext.lock) {
				data = testHarness.snapshot(snapshotId, System.currentTimeMillis());
				previousSnapshotId = lastSnapshotId;
				lastSnapshotId = messageId;
			}
			// let some time pass
			Thread.sleep(5);

			// check if the correct number of messages have been snapshotted
			final long numIds = lastSnapshotId - previousSnapshotId;

			RMQTestSource sourceCopy = new RMQTestSource();
			StreamSource<String, RMQTestSource> srcCopy = new StreamSource<>(sourceCopy);
			AbstractStreamOperatorTestHarness<String> testHarnessCopy =
				new AbstractStreamOperatorTestHarness<>(srcCopy, 1, 1, 0);

			testHarnessCopy.setup();
			testHarnessCopy.initializeState(data);
			testHarnessCopy.open();

			ArrayDeque<Tuple2<Long, Set<String>>> deque = sourceCopy.getRestoredState();
			Set<String> messageIds = deque.getLast().f1;

			assertEquals(numIds, messageIds.size());
			if (messageIds.size() > 0) {
				assertTrue(messageIds.contains(Long.toString(lastSnapshotId - 1)));
			}

			// check if the messages are being acknowledged and the transaction committed
			synchronized (DummySourceContext.lock) {
				source.notifyCheckpointComplete(snapshotId);
			}
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
	 * The source should not acknowledge ids in auto-commit mode or check for previously acknowledged ids.
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
	 * Tests error reporting in case of invalid correlation ids.
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

	/**
	 * Tests whether constructor params are passed correctly.
	 */
	@Test
	public void testConstructorParams() throws Exception {
		// verify construction params
		RMQConnectionConfig.Builder builder = new RMQConnectionConfig.Builder();
		builder.setHost("hostTest").setPort(999).setUserName("userTest").setPassword("passTest").setVirtualHost("/");
		ConstructorTestClass testObj = new ConstructorTestClass(
			builder.build(), "queueTest", false, new StringDeserializationScheme());

		try {
			testObj.open(new Configuration());
		} catch (Exception e) {
			// connection fails but check if args have been passed correctly
		}

		assertEquals("hostTest", testObj.getFactory().getHost());
		assertEquals(999, testObj.getFactory().getPort());
		assertEquals("userTest", testObj.getFactory().getUsername());
		assertEquals("passTest", testObj.getFactory().getPassword());
	}

	/**
	 * Tests getting the correct body and correlationID given which constructor was called.
	 * if the constructor with the {@link DeserializationSchema} was called it should extract the body of the message
	 * from the {@link Delivery} and the correlation ID from the {@link AMQP.BasicProperties} which are
	 * mocked to "I Love Turtles" and "0".
	 * if the constructor with the {@link RMQDeserializationSchema} was called it uses the
	 * {@link RMQDeserializationSchema#deserialize} method to parse the message and extract the correlation ID which
	 * both are implemented in {@link RMQTestSource#initAMQPMocks()} to return the
	 * {@link AMQP.BasicProperties#getMessageId()} that is mocked to return "1-MESSAGE_ID"
	 */
	@Test
	public void testProcessMessage() throws Exception {
		RMQTestSource source = new RMQTestSource();
		source.open(config);
		RMQDeserializationSchema.RMQCollector collector = Mockito.mock(RMQDeserializationSchema.RMQCollector.class);
		source.processMessage(source.mockedDelivery, collector);
		Mockito.verify(collector, Mockito.times(1)).collect("test");
		Mockito.verify(collector, Mockito.times(1)).setMessageIdentifiers("1", messageId);

		source = new RMQTestSource(new CustomDeserializationSchema());
		source.open(config);
		List<String> expectedOutput = new ArrayList<>(1);
		expectedOutput.add("I Love Turtles");
		expectedOutput.add("Brush your teeth");
		collector = Mockito.mock(RMQDeserializationSchema.RMQCollector.class);
		source.processMessage(source.mockedDelivery, collector);
		Mockito.verify(collector, Mockito.times(1)).collect(Mockito.eq(expectedOutput));
		Mockito.verify(collector, Mockito.times(1)).setMessageIdentifiers("1-MESSAGE_ID", messageId);
	}

	@Test
	public void testOpen() throws Exception {
		MockDeserializationSchema<String> deserializationSchema = new MockDeserializationSchema<>();

		RMQSource<String> consumer = new RMQTestSource(deserializationSchema);
		AbstractStreamOperatorTestHarness<String> testHarness = new AbstractStreamOperatorTestHarness<>(
			new StreamSource<>(consumer), 1, 1, 0
		);

		testHarness.open();
		assertThat("Open method was not called", deserializationSchema.isOpenCalled(), is(true));
	}

	@Test
	public void testOverrideConnection() throws Exception {
		final Connection mockConnection = Mockito.mock(Connection.class);
		Channel channel = Mockito.mock(Channel.class);
		Mockito.when(mockConnection.createChannel()).thenReturn(channel);

		RMQMockedRuntimeTestSource source = new RMQMockedRuntimeTestSource() {
			@Override
			protected Connection setupConnection() throws Exception {
				return mockConnection;
			}
		};

		FunctionInitializationContext mockContext = getMockContext();
		source.initializeState(mockContext);
		source.open(new Configuration());

		Mockito.verify(mockConnection, Mockito.times(1)).createChannel();
	}

	@Test
	public void testSetPrefetchCount() throws Exception {
		RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
			.setHost("localhost")
			.setPort(5000)
			.setUserName("guest")
			.setPassword("guest")
			.setVirtualHost("/")
			.setPrefetchCount(1000)
			.build();
		final Connection mockConnection = Mockito.mock(Connection.class);
		Channel channel = Mockito.mock(Channel.class);
		Mockito.when(mockConnection.createChannel()).thenReturn(channel);

		RMQMockedRuntimeTestSource source = new RMQMockedRuntimeTestSource(connectionConfig) {
			@Override
			protected Connection setupConnection() throws Exception {
				return mockConnection;
			}
		};

		FunctionInitializationContext mockContext = getMockContext();
		source.initializeState(mockContext);
		source.open(new Configuration());

		Mockito.verify(mockConnection, Mockito.times(1)).createChannel();
		Mockito.verify(channel, Mockito.times(1)).basicQos(1000, true);
	}

	@Test
	public void testUnsetPrefetchCount() throws Exception {
		final Connection mockConnection = Mockito.mock(Connection.class);
		Channel channel = Mockito.mock(Channel.class);
		Mockito.when(mockConnection.createChannel()).thenReturn(channel);

		RMQMockedRuntimeTestSource source = new RMQMockedRuntimeTestSource() {
			@Override
			protected Connection setupConnection() throws Exception {
				return mockConnection;
			}
		};

		FunctionInitializationContext mockContext = getMockContext();
		source.initializeState(mockContext);
		source.open(new Configuration());

		Mockito.verify(mockConnection, Mockito.times(1)).createChannel();
		Mockito.verify(channel, Mockito.times(0)).basicQos(anyInt());
	}

	private static class ConstructorTestClass extends RMQSource<String> {

		private ConnectionFactory factory;

		public ConstructorTestClass(RMQConnectionConfig rmqConnectionConfig,
									String queueName,
									boolean usesCorrelationId,
									DeserializationSchema<String> deserializationSchema) throws Exception {
			super(rmqConnectionConfig, queueName, usesCorrelationId, deserializationSchema);
			RMQConnectionConfig.Builder builder = new RMQConnectionConfig.Builder();
			builder.setHost("hostTest").setPort(999).setUserName("userTest").setPassword("passTest").setVirtualHost("/");
			factory = Mockito.spy(builder.build().getConnectionFactory());
			try {
				Mockito.doThrow(new RuntimeException()).when(factory).newConnection();
			} catch (IOException e) {
				fail("Failed to stub connection method");
			}
		}

		@Override
		protected ConnectionFactory setupConnectionFactory() {
			return factory;
		}

		public ConnectionFactory getFactory() {
			return factory;
		}
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
			return new String(message, ConfigConstants.DEFAULT_CHARSET);
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

	private class CustomDeserializationSchema implements RMQDeserializationSchema<String> {
		@Override
		public TypeInformation<String> getProducedType() {
			return TypeExtractor.getForClass(String.class);
		}

		@Override
		public void open(DeserializationSchema.InitializationContext context) throws Exception {

		}

		@Override
		public void deserialize(Envelope envelope, AMQP.BasicProperties properties, byte[] body, RMQCollector collector) throws IOException {
			List<String> messages = new ArrayList();
			messages.add("I Love Turtles");
			messages.add("Brush your teeth");
			collector.setMessageIdentifiers(properties.getMessageId(), envelope.getDeliveryTag());
			collector.collect(messages);
		}

		@Override
		public boolean isEndOfStream(String record) {
			return false;
		}
	}

	/**
	 * A base class of {@link RMQTestSource} for testing functions that rely on the {@link RuntimeContext}.
	 */
	private static class RMQMockedRuntimeTestSource extends RMQSource<String> {
		static final String QUEUE_NAME = "queueDummy";

		static final RMQConnectionConfig CONNECTION_CONFIG = new RMQConnectionConfig
			.Builder()
			.setHost("hostTest")
			.setPort(999)
			.setUserName("userTest")
			.setPassword("passTest")
			.setVirtualHost("/")
			.build();

		protected RuntimeContext runtimeContext = Mockito.mock(StreamingRuntimeContext.class);

		public RMQMockedRuntimeTestSource(RMQConnectionConfig connectionConfig, DeserializationSchema<String> deserializationSchema) {
			super(connectionConfig, QUEUE_NAME, true, deserializationSchema);
		}

		public RMQMockedRuntimeTestSource(RMQConnectionConfig connectionConfig, RMQDeserializationSchema<String> deliveryParser) {
			super(connectionConfig, QUEUE_NAME, true, deliveryParser);
		}

		public RMQMockedRuntimeTestSource(DeserializationSchema<String> deserializationSchema) {
			this(CONNECTION_CONFIG, deserializationSchema);
		}

		public RMQMockedRuntimeTestSource(RMQDeserializationSchema<String> deliveryParser){
			this(CONNECTION_CONFIG, deliveryParser);
		}

		public RMQMockedRuntimeTestSource(RMQConnectionConfig connectionConfig) {
			this(connectionConfig, new StringDeserializationScheme());
		}

		public RMQMockedRuntimeTestSource() {
			this(new StringDeserializationScheme());
		}

		@Override
		public RuntimeContext getRuntimeContext() {
			return runtimeContext;
		}
	}

	private class RMQTestSource extends RMQMockedRuntimeTestSource {
		private ArrayDeque<Tuple2<Long, Set<String>>> restoredState;

		private Delivery mockedDelivery;
		public Envelope mockedAMQPEnvelope;
		public AMQP.BasicProperties mockedAMQPProperties;

		public RMQTestSource() {
			super();
		}

		public RMQTestSource(DeserializationSchema<String> deserializationSchema) {
			super(deserializationSchema);
		}

		public RMQTestSource(RMQDeserializationSchema deliveryParser) {
			super(deliveryParser);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			super.initializeState(context);
			this.restoredState = this.pendingCheckpoints;
		}

		public ArrayDeque<Tuple2<Long, Set<String>>> getRestoredState() {
			return this.restoredState;
		}

		public void initAMQPMocks() {
			consumer = Mockito.mock(QueueingConsumer.class);

			// Mock for delivery
			mockedDelivery = Mockito.mock(Delivery.class);
			Mockito.when(mockedDelivery.getBody()).thenReturn("test".getBytes(ConfigConstants.DEFAULT_CHARSET));

			try {
				Mockito.when(consumer.nextDelivery()).thenReturn(mockedDelivery);
			} catch (InterruptedException e) {
				fail("Couldn't setup up deliveryMock");
			}

			// Mock for envelope
			mockedAMQPEnvelope = Mockito.mock(Envelope.class);
			Mockito.when(mockedDelivery.getEnvelope()).thenReturn(mockedAMQPEnvelope);

			Mockito.when(mockedAMQPEnvelope.getDeliveryTag()).thenAnswer(new Answer<Long>() {
				@Override
				public Long answer(InvocationOnMock invocation) throws Throwable {
					return ++messageId;
				}
			});

			// Mock for properties
			mockedAMQPProperties = Mockito.mock(AMQP.BasicProperties.class);
			Mockito.when(mockedDelivery.getProperties()).thenReturn(mockedAMQPProperties);

			Mockito.when(mockedAMQPProperties.getCorrelationId()).thenAnswer(new Answer<String>() {
				@Override
				public String answer(InvocationOnMock invocation) throws Throwable {
					return generateCorrelationIds ? "" + messageId : null;
				}
			});

			Mockito.when(mockedAMQPProperties.getMessageId()).thenAnswer(new Answer<String>(){
				@Override
				public String answer(InvocationOnMock invocation) throws Throwable {
					return messageId + "-MESSAGE_ID";
				}
			});
		}

		@Override
		public void open(Configuration config) throws Exception {
			super.open(config);
			initAMQPMocks();
		}

		@Override
		protected ConnectionFactory setupConnectionFactory() {
			ConnectionFactory connectionFactory = Mockito.mock(ConnectionFactory.class);
			Connection connection = Mockito.mock(Connection.class);
			try {
				Mockito.when(connectionFactory.newConnection()).thenReturn(connection);
				Mockito.when(connection.createChannel()).thenReturn(Mockito.mock(Channel.class));
			} catch (IOException | TimeoutException e) {
				fail("Test environment couldn't be created.");
			}
			return connectionFactory;
		}

		@Override
		public void setRuntimeContext(RuntimeContext t) {
			this.runtimeContext = t;
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
			throw new UnsupportedOperationException();
		}

		@Override
		public void markAsTemporarilyIdle() {
			throw new UnsupportedOperationException();
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
