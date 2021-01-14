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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.atomic.AtomicRowDataDeserializationSchema;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.streaming.connectors.pulsar.internal.AvroDeser;
import org.apache.flink.streaming.connectors.pulsar.internal.CachedPulsarClient;
import org.apache.flink.streaming.connectors.pulsar.internal.IncompatibleSchemaException;
import org.apache.flink.streaming.connectors.pulsar.internal.JsonDeser;
import org.apache.flink.streaming.connectors.pulsar.internal.ReaderThread;
import org.apache.flink.streaming.connectors.pulsar.internal.SimpleSchemaTranslator;
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.pulsar.testutils.SingletonStreamSink;
import org.apache.flink.streaming.connectors.pulsar.testutils.ValidatingExactlyOnceSink;
import org.apache.flink.streaming.connectors.pulsar.util.RowDataUtil;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.serialization.FlinkSchema;
import org.apache.flink.streaming.util.serialization.PulsarDeserializationSchema;
import org.apache.flink.streaming.util.serialization.PulsarPrimitiveSchema;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchema;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchemaWrapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.test.util.TestUtils;
import org.apache.flink.testutils.junit.RetryRule;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.connectors.pulsar.SchemaData.faList;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.fooList;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.CLIENT_CACHE_SIZE_OPTION_KEY;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.FAIL_ON_WRITE_OPTION_KEY;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.FLUSH_ON_CHECKPOINT_OPTION_KEY;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.PARTITION_DISCOVERY_INTERVAL_MS_OPTION_KEY;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.TOPIC_ATTRIBUTE_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.TOPIC_MULTI_OPTION_KEY;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.TOPIC_SINGLE_OPTION_KEY;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.USE_EXTEND_FIELD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Pulsar source sink integration tests.
 */
@Slf4j
public class FlinkPulsarITest extends PulsarTestBaseWithFlink {

	@Rule
	public RetryRule retryRule = new RetryRule();

	@Before
	public void clearState() {
		SingletonStreamSink.clear();
		FailingIdentityMapper.failedBefore = false;
	}

	@Test(timeout = 40 * 1000L)
	public void testRunFailedOnWrongServiceUrl() {

		try {
			Properties props = MapUtils.toProperties(Collections.singletonMap(
				TOPIC_SINGLE_OPTION_KEY,
				"tp"));

			StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
			see.setRestartStrategy(RestartStrategies.noRestart());
			see.setParallelism(1);

			FlinkPulsarSource<String> source =
				new FlinkPulsarSource<String>(
					"sev",
					"admin",
					new PulsarPrimitiveSchema<>(String.class),
					props)
					.setStartFromEarliest();

			DataStream<String> stream = see.addSource(source);
			stream.print();
			see.execute("wrong service url");
		} catch (Exception e) {
			final Optional<Throwable> optionalThrowable =
				ExceptionUtils.findThrowableWithMessage(e, "authority component is missing");
			assertTrue(optionalThrowable.isPresent());
			assertTrue(optionalThrowable.get() instanceof IllegalArgumentException);
		}
	}

	@Test(timeout = 40 * 1000L)
	public void testCaseSensitiveReaderConf() throws Exception {
		String tp = newTopic();
		List<Integer> messages =
			IntStream.range(0, 50).mapToObj(t -> Integer.valueOf(t)).collect(Collectors.toList());

		sendTypedMessages(tp, SchemaType.INT32, messages, Optional.empty());
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = MapUtils.toProperties(Collections.singletonMap(
			TOPIC_SINGLE_OPTION_KEY,
			tp));
		props.setProperty("pulsar.reader.receiverQueueSize", "1000000");

		FlinkPulsarSource<Integer> source =
			new FlinkPulsarSource<Integer>(
				serviceUrl,
				adminUrl,
				new PulsarPrimitiveSchema(Integer.TYPE),
				props)
				.setStartFromEarliest();

		DataStream<Integer> stream = see.addSource(source);
		stream.addSink(new DiscardingSink<Integer>());

		final AtomicReference<Throwable> jobError = new AtomicReference<>();
		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(see.getStreamGraph());
		JobID jobID = jobGraph.getJobID();

		final Thread runner = new Thread("runner") {
			@Override
			public void run() {
				try {
					//client.setDetached(false);
					client.submitJob(jobGraph);
				} catch (Throwable t) {
					if (!(t instanceof JobCancellationException)) {
						jobError.set(t);
					}
				}
			}
		};
		runner.start();

		Thread.sleep(2000);
		Throwable failureCause = jobError.get();

		if (failureCause != null) {
			failureCause.printStackTrace();
			fail("Test failed prematurely with: " + failureCause.getMessage());
		}

		client.cancel(jobID);
		runner.join();
		Thread.sleep(3000);
		assertEquals(client.getJobStatus(jobID).get(), JobStatus.CANCELED);
	}

	@Test(timeout = 40 * 1000L)
	public void testCaseSensitiveProducerConf() throws Exception {
		int numTopic = 5;
		int numElements = 20;

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(1);

		List<String> topics = new ArrayList<>();
		for (int i = 0; i < numTopic; i++) {
			topics.add(newTopic());
		}

		DataStream<RowData> stream = see.addSource(new MultiTopicSource(topics, numElements));

		Properties sinkProp = sinkProperties();
		sinkProp.setProperty("pulsar.producer.blockIfQueueFull", "true");
		sinkProp.setProperty("pulsar.producer.maxPendingMessages", "100000");
		sinkProp.setProperty("pulsar.producer.maxPendingMessagesAcrossPartitions", "5000000");
		sinkProp.setProperty("pulsar.producer.sendTimeoutMs", "30000");
		produceIntoPulsar(stream, intRowType(), sinkProp, intRowTypeInfo());
		see.execute("write with topics");
	}

	@Test(timeout = 40 * 1000L)
	public void testClientCacheParameterPassedToTasks() throws Exception {
		int numTopic = 5;
		int numElements = 20;

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(3);

		List<String> topics = new ArrayList<>();
		for (int i = 0; i < numTopic; i++) {
			topics.add(newTopic());
		}

		DataStream<RowData> stream = see.addSource(new MultiTopicSource(topics, numElements));

		Properties sinkProp = sinkProperties();
		sinkProp.setProperty(FLUSH_ON_CHECKPOINT_OPTION_KEY, "true");
		sinkProp.setProperty(CLIENT_CACHE_SIZE_OPTION_KEY, "7");
		stream.addSink(new AssertSink(serviceUrl, adminUrl, 7, sinkProp, intRowWithTopicType()));
		see.execute("write with topics");
	}

	@Test(timeout = 40 * 1000L)
	public void testProduceConsumeMultipleTopics() throws Exception {
		int numTopic = 5;
		int numElements = 20;

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(1);

		List<String> topics = new ArrayList<>();
		for (int i = 0; i < numTopic; i++) {
			topics.add(newTopic());
		}

		DataStream<RowData> stream = see.addSource(new MultiTopicSource(topics, numElements));

		Properties sinkProp = sinkProperties();
		produceIntoPulsar(stream, intRowType(), sinkProp, intRowTypeInfo());
		see.execute("write with topics");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Properties sourceProp = sourceProperties();
		sourceProp.setProperty(TOPIC_MULTI_OPTION_KEY, StringUtils.join(topics.toArray(), ','));
		sourceProp.setProperty(USE_EXTEND_FIELD, "true");

		final DeserializationSchema<RowData> jsonRowDataDerSchema = getJsonRowDataDerSchema((RowType) intRowWithTopicType()
			.getLogicalType());
		DataStream<RowData> stream1 = env.addSource(
			new FlinkPulsarSource<RowData>(
				serviceUrl,
				adminUrl,
				PulsarDeserializationSchema.valueOnly(jsonRowDataDerSchema),
				sourceProp)
				.setStartFromEarliest());

		stream1.flatMap(new CountMessageNumberFM(numElements, 1)).setParallelism(1);
		TestUtils.tryExecute(env, "count elements from topics");
	}

	@Test(timeout = 40 * 10000L)
	public void testCommitOffsetsToPulsar() throws Exception {
		int numTopic = 3;
		List<Integer> messages = IntStream.range(0, 50).boxed().collect(Collectors.toList());
		List<String> topics = new ArrayList<>();
		Map<String, MessageId> expectedIds = new HashMap<>();

		for (int i = 0; i < numTopic; i++) {
			String topic = newTopic();
			topics.add(topic);
			List<MessageId> ids = sendTypedMessages(
				topic,
				SchemaType.INT32,
				messages,
				Optional.empty());
			expectedIds.put(topic, ids.get(ids.size() - 1));
		}

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(3);
		see.enableCheckpointing(200);

		String subName = UUID.randomUUID().toString();

		Properties sourceProps = sourceProperties();
		sourceProps.setProperty(TOPIC_MULTI_OPTION_KEY, StringUtils.join(topics, ','));

		DataStream stream = see.addSource(
			new FlinkPulsarRowSourceSub(
				subName,
				serviceUrl,
				adminUrl,
				sourceProps).setStartFromEarliest());
		stream.addSink(new DiscardingSink());

		AtomicReference<Throwable> error = new AtomicReference<>();

		Thread runner = new Thread("runner") {
			@Override
			public void run() {
				try {
					see.execute();
				} catch (Throwable e) {
					if (!(e instanceof JobCancellationException)) {
						error.set(e);
					}
				}
			}
		};
		runner.start();

		Thread.sleep(3000);

		Long deadline = 30000000000L + System.nanoTime();

		boolean gotLast = false;

		do {
			Map<String, MessageId> ids = getCommittedOffsets(topics, subName);
			if (roughEquals(ids, expectedIds)) {
				gotLast = true;
			} else {
				Thread.sleep(100);
			}
		} while (System.nanoTime() < deadline && !gotLast);

		client.cancel(Iterables.getOnlyElement(getRunningJobs(client)));
		runner.join();

		Throwable t = error.get();
		if (t != null) {
			fail("Job failed with an exception " + ExceptionUtils.stringifyException(t));
		}

		assertTrue(gotLast);
	}

	@Test(timeout = 40 * 1000L)
	public void testAvro() throws Exception {
		String topic = newTopic();

		sendTypedMessages(topic, SchemaType.AVRO, fooList, Optional.empty(), SchemaData.Foo.class);

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(1);
		see.setRestartStrategy(RestartStrategies.noRestart());

		Properties sourceProps = sourceProperties();
		sourceProps.setProperty(TOPIC_SINGLE_OPTION_KEY, topic);

		FlinkPulsarSource<SchemaData.Foo> source =
			new FlinkPulsarSource<>(serviceUrl, adminUrl,
				PulsarDeserializationSchema.valueOnly(AvroDeser.of(SchemaData.Foo.class)),
				sourceProps)
				.setStartFromEarliest();

		DataStream<Integer> ds = see.addSource(source)
			.map(SchemaData.Foo::getI);

		ds.map(new FailingIdentityMapper<>(fooList.size()))
			.addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

		TestUtils.tryExecute(see, "test read data of POJO using avro");

		SingletonStreamSink.compareWithList(
			fooList
				.subList(0, fooList.size() - 1)
				.stream()
				.map(SchemaData.Foo::getI)
				.map(Objects::toString)
				.collect(Collectors.toList()));
	}

	@Test(timeout = 40 * 1000L)
	public void testJson() throws Exception {
		String topic = newTopic();

		sendTypedMessages(topic, SchemaType.JSON, fooList, Optional.empty(), SchemaData.Foo.class);

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(1);
		see.setRestartStrategy(RestartStrategies.noRestart());

		//String subName = UUID.randomUUID().toString();

		Properties sourceProps = sourceProperties();
		sourceProps.setProperty(TOPIC_SINGLE_OPTION_KEY, topic);

		FlinkPulsarSource<SchemaData.Foo> source =
			new FlinkPulsarSource<>(serviceUrl, adminUrl,
				PulsarDeserializationSchema.valueOnly(JsonDeser.of(SchemaData.Foo.class)),
				sourceProps)
				.setStartFromEarliest();

		DataStream<Integer> ds = see.addSource(source)
			.map(SchemaData.Foo::getI);

		ds.map(new FailingIdentityMapper<>(fooList.size()))
			.addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

		try {
			see.execute("test read data of POJO using JSON");
		} catch (Exception e) {
		}
		SingletonStreamSink.compareWithList(
			fooList
				.subList(0, fooList.size() - 1)
				.stream()
				.map(SchemaData.Foo::getI)
				.map(Objects::toString)
				.collect(Collectors.toList()));
	}

	@Test(timeout = 40 * 1000L)
	public void testStartFromEarliest() throws Exception {
		int numTopic = 3;
		List<Integer> messages = IntStream.range(0, 50).boxed().collect(Collectors.toList());
		List<String> topics = new ArrayList<>();
		Map<String, Set<Integer>> expectedData = new HashMap<>();

		for (int i = 0; i < numTopic; i++) {
			String topic = newTopic();
			topics.add(topic);
			sendTypedMessages(topic, SchemaType.INT32, messages, Optional.empty());
			expectedData.put(topic, new HashSet<>(messages));
		}

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(3);

		Properties sourceProps = sourceProperties();
		sourceProps.setProperty(TOPIC_MULTI_OPTION_KEY, StringUtils.join(topics, ','));
		sourceProps.setProperty(USE_EXTEND_FIELD, "true");

		final PulsarDeserializationSchema<RowData> deserializationSchema =
			new RowDataPulsarDeserializationSchema((AtomicDataType) DataTypes.INT());
		DataStream stream = see.addSource(
			new FlinkPulsarSource<>(serviceUrl, adminUrl, deserializationSchema, sourceProps)
				.setStartFromEarliest());

		stream.flatMap(new CheckAllMessageExist(expectedData, 150, 1)).setParallelism(1);

		TestUtils.tryExecute(see, "start from earliest");
	}

	@Test(timeout = 40 * 1000L)
	public void testStartFromLatest() throws Exception {
		int numTopic = 3;
		List<Integer> messages = IntStream.range(0, 50).boxed().collect(Collectors.toList());
		List<Integer> newMessages = IntStream.range(50, 60).boxed().collect(Collectors.toList());
		Map<String, Set<Integer>> expectedData = new HashMap<>();

		List<String> topics = new ArrayList<>();

		for (int i = 0; i < numTopic; i++) {
			String topic = newTopic();
			topics.add(topic);
			sendTypedMessages(topic, SchemaType.INT32, messages, Optional.empty());
			expectedData.put(topic, new HashSet<>(newMessages));
		}

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(3);

		Properties sourceProps = sourceProperties();
		sourceProps.setProperty(TOPIC_MULTI_OPTION_KEY, StringUtils.join(topics, ','));
		sourceProps.setProperty(USE_EXTEND_FIELD, "true");
		PulsarDeserializationSchema<RowData> deserializationSchema =
			new RowDataPulsarDeserializationSchema((AtomicDataType) DataTypes.INT());
		see.addSource(
			new FlinkPulsarSource<>(
				serviceUrl,
				adminUrl,
				deserializationSchema,
				sourceProps).setStartFromLatest())
			.flatMap(new CheckAllMessageExist(expectedData, 30, 1)).setParallelism(1)
			.addSink(new DiscardingSink());

		AtomicReference<Throwable> error = new AtomicReference<>();
		Thread consumerThread = new Thread() {
			@Override
			public void run() {
				try {
					see.execute("testStartFromLatest");
				} catch (Throwable e) {
					if (!ExceptionUtils
						.findThrowable(e, JobCancellationException.class)
						.isPresent()) {
						error.set(e);
					}
				}
			}
		};
		consumerThread.start();

		Thread.sleep(3000);

		Thread extraProduceThread = new Thread() {
			@Override
			public void run() {
				try {
					for (String tp : topics) {
						sendTypedMessages(tp, SchemaType.INT32, newMessages, Optional.empty());
					}
				} catch (PulsarClientException e) {
					throw new RuntimeException(e);
				}
			}
		};
		extraProduceThread.start();

		consumerThread.join();

		Throwable consumerError = error.get();
		assertTrue(ExceptionUtils.findThrowable(consumerError, SuccessException.class).isPresent());
	}

	@Test(timeout = 40 * 1000L)
	public void testStartFromSpecific() throws Exception {
		String topic = newTopic();
		List<MessageId> mids = sendTypedMessages(topic, SchemaType.INT32, Arrays.asList(
			//  0,   1,   2, 3, 4, 5,  6,  7,  8
			-20, -21, -22, 1, 2, 3, 10, 11, 12), Optional.empty());

		Map<String, Set<Integer>> expectedData = new HashMap<>();
		expectedData.put(topic, new HashSet<>(Arrays.asList(2, 3, 10, 11, 12)));

		Map<String, MessageId> offset = new HashMap<>();
		offset.put(topic, mids.get(3));

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(1);

		Properties sourceProps = sourceProperties();
		sourceProps.setProperty(TOPIC_SINGLE_OPTION_KEY, topic);
		sourceProps.setProperty(USE_EXTEND_FIELD, "true");

		final PulsarDeserializationSchema<RowData> deserializationSchema =
			new RowDataPulsarDeserializationSchema((AtomicDataType) DataTypes.INT());
		DataStream stream = see.addSource(
			new FlinkPulsarSource<>(serviceUrl, adminUrl, deserializationSchema, sourceProps)
				.setStartFromSpecificOffsets(offset));
		stream.flatMap(new CheckAllMessageExist(expectedData, 5, 1)).setParallelism(1);

		TestUtils.tryExecute(see, "start from specific");
	}

	@Test(timeout = 40 * 1000L)
	public void testStartFromExternalSubscription() throws Exception {
		String topic = newTopic();
		List<MessageId> mids = sendTypedMessages(topic, SchemaType.INT32, Arrays.asList(
			//  0,   1,   2, 3, 4, 5,  6,  7,  8
			-20, -21, -22, 1, 2, 3, 10, 11, 12), Optional.empty());

		String subName = "sub-1";

		PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();

		admin.topics().createSubscription(TopicName.get(topic).toString(), subName, mids.get(3));

		Map<String, Set<Integer>> expectedData = new HashMap<>();
		expectedData.put(topic, new HashSet<>(Arrays.asList(2, 3, 10, 11, 12)));

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(1);

		Properties sourceProps = sourceProperties();
		sourceProps.setProperty(TOPIC_SINGLE_OPTION_KEY, topic);
		sourceProps.setProperty(USE_EXTEND_FIELD, "true");

		final PulsarDeserializationSchema<RowData> deserializationSchema =
			new RowDataPulsarDeserializationSchema((AtomicDataType) DataTypes.INT());
		DataStream stream =
			see.addSource(new FlinkPulsarSource<>(
				serviceUrl,
				adminUrl,
				deserializationSchema,
				sourceProps)
				.setStartFromSubscription(subName));
		stream.flatMap(new CheckAllMessageExist(expectedData, 5, 1)).setParallelism(1);

		TestUtils.tryExecute(see, "start from specific");

		assertTrue(Sets.newHashSet(admin.topics().getSubscriptions(topic)).contains(subName));

		admin.close();
	}

	@Test
	public void testWriteAndReadPojo() throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(1);

		String topic = newTopic();
		DataStreamSource<SchemaData.FA> rowDataStreamSource = see.addSource(new PojoSeq(10));
		ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
		clientConfigurationData.setServiceUrl(serviceUrl);
		rowDataStreamSource.addSink(new FlinkPulsarSink(adminUrl, Optional.of(topic),
			clientConfigurationData, new Properties(),
			new PulsarSerializationSchemaWrapper.Builder<>(new SerializationSchema<SchemaData.FA>() {
				@Override
				public byte[] serialize(SchemaData.FA element) {
					return Schema.JSON(SchemaData.FA.class).encode(element);
				}
			}).usePojoMode(SchemaData.FA.class, RecordSchemaType.JSON).build()
		));
		TestUtils.tryExecute(see, "one to one exactly once test");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		Properties properties = new Properties();
		properties.put(TOPIC_SINGLE_OPTION_KEY, topic);
		FlinkPulsarSource<SchemaData.FA> integerFlinkPulsarSource = new FlinkPulsarSource<>(
			serviceUrl,
			adminUrl,
			PulsarDeserializationSchema.valueOnly(new DeserializationSchema<SchemaData.FA>() {
				@Override
				public SchemaData.FA deserialize(byte[] message) throws IOException {
					return Schema.JSON(SchemaData.FA.class).decode(message);
				}

				@Override
				public boolean isEndOfStream(SchemaData.FA nextElement) {
					return false;
				}

				@Override
				public TypeInformation<SchemaData.FA> getProducedType() {
					return TypeInformation.of(SchemaData.FA.class);
				}
			}),
			properties).setStartFromEarliest();

		//env.addSource(integerFlinkPulsarSource).addSink(new PrintSinkFunction<>());
		//TestUtils.tryExecute(env, "one to one exactly once test");

		DataStream<SchemaData.FA> ds = env.addSource(integerFlinkPulsarSource);

		ds.map(new FailingIdentityMapper<>(faList.size()))
			.addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

		try {
			env.execute("test read data of POJO using JSON");
		} catch (Exception e) {
		}
		SingletonStreamSink.compareWithList(
			faList
				.subList(0, faList.size() - 1)
				.stream()
				.map(Objects::toString)
				.collect(Collectors.toList()));
	}

	//TODO test pojo
	@Test(timeout = 40 * 1000L)
	public void testSourceAndSink() throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(1);

		int numElements = 10;
		String topic = newTopic();
		List<Integer> expectList = IntStream.range(0, numElements)
			.mapToObj(value -> Integer.valueOf(value))
			.collect(Collectors.toList());
		DataStreamSource<Integer> rowDataStreamSource = see.fromCollection(expectList);
		ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
		clientConfigurationData.setServiceUrl(serviceUrl);

		rowDataStreamSource.addSink(new FlinkPulsarSink(adminUrl, Optional.of(topic),
			clientConfigurationData, new Properties(),
			new PulsarSerializationSchemaWrapper.Builder<>
				((SerializationSchema<Integer>) element -> Schema.INT32.encode(element))
				.useAtomicMode(DataTypes.INT())
				.build()));
		TestUtils.tryExecute(see, "test sink data to pulsar");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		Properties properties = new Properties();
		properties.put(TOPIC_SINGLE_OPTION_KEY, topic);
		FlinkPulsarSource<Integer> integerFlinkPulsarSource = new FlinkPulsarSource<>(
			serviceUrl,
			adminUrl,
			new PulsarPrimitiveSchema<>(Integer.class),
			properties).setStartFromEarliest();
		env
			.addSource(integerFlinkPulsarSource)
			.map(Object::toString)
			.map(new FailingIdentityMapper<>(expectList.size()))
			.addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

		TestUtils.tryExecute(env, "test read data from pulsar");

		SingletonStreamSink.compareWithList(expectList
			.subList(0, expectList.size() - 1)
			.stream()
			.map(Object::toString)
			.collect(Collectors.toList()));
	}

	@Test(timeout = 40 * 1000L)
	public void testOne2OneExactlyOnce() throws Exception {
		String topic = newTopic();
		int parallelism = 5;
		int numElementsPerPartition = 1000;
		int totalElements = parallelism * numElementsPerPartition;
		int failAfterElements = numElementsPerPartition / 3;

		List<String> allTopicNames = new ArrayList<>();
		for (int i = 0; i < parallelism; i++) {
			allTopicNames.add(topic + "-partition-" + i);
		}

		createTopic(topic, parallelism, adminUrl);

		generateRandomizedIntegerSequence(
			StreamExecutionEnvironment.getExecutionEnvironment(),
			topic,
			parallelism,
			numElementsPerPartition,
			true);

		// run the topology that fails and recovers

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(500);
		env.setParallelism(parallelism);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
		Properties sourceProps = sourceProperties();
		sourceProps.setProperty(TOPIC_MULTI_OPTION_KEY, StringUtils.join(allTopicNames, ','));
		sourceProps.setProperty(USE_EXTEND_FIELD, "true");

		final DeserializationSchema<RowData> dataDerSchema =
			getJsonRowDataDerSchema((RowType) intRowWithTopicType().getLogicalType());
		PulsarDeserializationSchema<RowData> deserializationSchema =
			PulsarDeserializationSchema.valueOnly(dataDerSchema);

		FlinkPulsarSource<RowData> flinkPulsarSource = new FlinkPulsarSource<>(
			serviceUrl,
			adminUrl,
			deserializationSchema,
			sourceProps
		);
		env.addSource(flinkPulsarSource.setStartFromEarliest())
			.map(new PartitionValidationMapper(parallelism, 1))
			.map(new FailingIdentityMapper<>(failAfterElements))
			.addSink(new ValidatingExactlyOnceSink(totalElements)).setParallelism(1);

		FailingIdentityMapper.failedBefore = false;
		TestUtils.tryExecute(env, "one to one exactly once test");
	}

	@Test(timeout = 40 * 1000L)
	public void testOne2MultiSource() throws Exception {
		String topic = newTopic();
		int numPartitions = 5;
		int numElementsPerPartition = 1000;
		int totalElements = numPartitions * numElementsPerPartition;
		int failAfterElements = numElementsPerPartition / 3;

		List<String> allTopicNames = new ArrayList<>();
		for (int i = 0; i < numPartitions; i++) {
			allTopicNames.add(topic + "-partition-" + i);
		}

		createTopic(topic, numPartitions, adminUrl);

		generateRandomizedIntegerSequence(
			StreamExecutionEnvironment.getExecutionEnvironment(),
			topic,
			numPartitions,
			numElementsPerPartition,
			true);

		int parallelism = 2;

		// run the topology that fails and recovers

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(500);
		env.setParallelism(parallelism);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

		Properties sourceProps = sourceProperties();
		sourceProps.setProperty(TOPIC_MULTI_OPTION_KEY, StringUtils.join(allTopicNames, ','));
		sourceProps.setProperty(USE_EXTEND_FIELD, "true");

		final DeserializationSchema<RowData> dataDerSchema =
			getJsonRowDataDerSchema((RowType) intRowWithTopicType().getLogicalType());
		PulsarDeserializationSchema<RowData> deserializationSchema =
			PulsarDeserializationSchema.valueOnly(dataDerSchema);

		FlinkPulsarSource<RowData> flinkPulsarSource = new FlinkPulsarSource<>(
			serviceUrl,
			adminUrl,
			deserializationSchema,
			sourceProps
		);
		env.addSource(flinkPulsarSource.setStartFromEarliest())
			.map(new PartitionValidationMapper(parallelism, 3))
			.map(new FailingIdentityMapper<>(failAfterElements))
			.addSink(new ValidatingExactlyOnceSink(totalElements)).setParallelism(1);

		FailingIdentityMapper.failedBefore = false;
		TestUtils.tryExecute(env, "One-source-multi-partitions exactly once test");
	}

	@Test(timeout = 40 * 1000L)
	public void testTaskNumberGreaterThanPartitionNumber() throws Exception {
		String topic = newTopic();
		int numPartitions = 5;
		int numElementsPerPartition = 1000;
		int totalElements = numPartitions * numElementsPerPartition;
		int failAfterElements = numElementsPerPartition / 3;

		List<String> allTopicNames = new ArrayList<>();
		for (int i = 0; i < numPartitions; i++) {
			allTopicNames.add(topic + "-partition-" + i);
		}

		createTopic(topic, numPartitions, adminUrl);

		generateRandomizedIntegerSequence(
			StreamExecutionEnvironment.getExecutionEnvironment(),
			topic,
			numPartitions,
			numElementsPerPartition,
			true);

		int parallelism = 8;

		// run the topology that fails and recovers

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(500);
		env.setParallelism(parallelism);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

		Properties sourceProps = sourceProperties();
		sourceProps.setProperty(TOPIC_MULTI_OPTION_KEY, StringUtils.join(allTopicNames, ','));
		sourceProps.setProperty(USE_EXTEND_FIELD, "true");

		final DeserializationSchema<RowData> dataDerSchema =
			getJsonRowDataDerSchema((RowType) intRowWithTopicType().getLogicalType());
		PulsarDeserializationSchema<RowData> deserializationSchema =
			PulsarDeserializationSchema.valueOnly(dataDerSchema);
		FlinkPulsarSource<RowData> flinkPulsarSource = new FlinkPulsarSource<>(
			serviceUrl,
			adminUrl,
			deserializationSchema,
			sourceProps
		);
		env.addSource(flinkPulsarSource.setStartFromEarliest())
			.map(new PartitionValidationMapper(parallelism, 1))
			.map(new FailingIdentityMapper<>(failAfterElements))
			.addSink(new ValidatingExactlyOnceSink(totalElements)).setParallelism(1);

		FailingIdentityMapper.failedBefore = false;
		TestUtils.tryExecute(env, "source task number > partition number");
	}

	@Test(timeout = 40 * 1000L)
	public void testCancelingOnFullInput() throws Exception {
		String tp = newTopic();
		int parallelism = 3;
		createTopic(tp, parallelism, adminUrl);

		InfiniteStringGenerator generator = new InfiniteStringGenerator(tp);
		generator.start();

		// launch a consumer asynchronously
		AtomicReference<Throwable> jobError = new AtomicReference<>();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.enableCheckpointing(100);

		Properties prop = sourceProperties();
		prop.setProperty(TOPIC_SINGLE_OPTION_KEY, tp);
		env.addSource(new FlinkPulsarSource<String>(
			serviceUrl,
			adminUrl,
			PulsarDeserializationSchema.valueOnly(new SimpleStringSchema()),
			prop).setStartFromEarliest())
			.addSink(new DiscardingSink<>());

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());
		JobID jobid = jobGraph.getJobID();

		Thread jobRunner = new Thread("program runner thread") {
			@Override
			public void run() {
				try {
					//client.setDetached(false);
					client.submitJob(jobGraph);
				} catch (Throwable e) {
					jobError.set(e);
				}
			}
		};
		jobRunner.start();

		Thread.sleep(2000);
		Throwable failureCause = jobError.get();

		if (failureCause != null) {
			failureCause.printStackTrace();
			fail("Test failed prematurely with: " + failureCause.getMessage());
		}

		client.cancel(jobid);

		jobRunner.join();
		Thread.sleep(2000);
		assertEquals(client.getJobStatus(jobid).get(), JobStatus.CANCELED);

		if (generator.isAlive()) {
			generator.shutdown();
			generator.join();
		} else {
			Throwable t = generator.getError();
			if (t != null) {
				t.printStackTrace();
				fail("Generator failed " + t.getMessage());
			} else {
				fail("Generator failed with no exception");
			}
		}
	}

	@Test(timeout = 40 * 1000L)
	public void testOnEmptyInput() throws Exception {
		String tp = newTopic();
		int parallelism = 3;
		createTopic(tp, parallelism, adminUrl);

		// launch a consumer asynchronously

		AtomicReference<Throwable> jobError = new AtomicReference<>();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.enableCheckpointing(100);

		Properties prop = sourceProperties();
		prop.setProperty(TOPIC_SINGLE_OPTION_KEY, tp);

		env
			.addSource(new FlinkPulsarSource<Integer>(
				serviceUrl,
				adminUrl,
				new PulsarPrimitiveSchema<>(Integer.class),
				prop).setStartFromEarliest())
			.addSink(new DiscardingSink<>());

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());
		JobID jobid = jobGraph.getJobID();

		Thread jobRunner = new Thread("program runner thread") {
			@Override
			public void run() {
				try {
					//client.setDetached(false);
					client.submitJob(jobGraph);
				} catch (Throwable e) {
					jobError.set(e);
				}
			}
		};
		jobRunner.start();

		Thread.sleep(2000);
		Throwable failureCause = jobError.get();

		if (failureCause != null) {
			failureCause.printStackTrace();
			fail("Test failed prematurely with: " + failureCause.getMessage());
		}

		client.cancel(jobid);
		Thread.sleep(3000);
		jobRunner.join();

		assertEquals(client.getJobStatus(jobid).get(), JobStatus.CANCELED);

	}

	public static boolean isCause(
		Class<? extends Throwable> expected,
		Throwable exc) {
		return expected.isInstance(exc) || (
			exc != null && isCause(expected, exc.getCause())
		);
	}

	private static class MultiTopicSource extends RichParallelSourceFunction<RowData> {
		private final List<String> topics;
		private final int numElements;
		private final int base;

		public MultiTopicSource(List<String> topics, int numElements, int base) {
			this.topics = topics;
			this.numElements = numElements;
			this.base = base;
		}

		public MultiTopicSource(List<String> topics, int numElements) {
			this(topics, numElements, 0);
		}

		@Override
		public void run(SourceContext<RowData> ctx) throws Exception {
			for (String topic : topics) {
				for (int i = 0; i < numElements; i++) {
					ctx.collect(intRowDataWithTopic(i + base, topic));
				}
			}
		}

		public RowData intRowDataWithTopic(int i, String tp) {
			final GenericRowData rowData = new GenericRowData(RowKind.INSERT, 2);
			RowDataUtil.setField(rowData, 0, i);
			RowDataUtil.setField(rowData, 1, tp);
			return rowData;
		}

		@Override
		public void cancel() {

		}
	}

	private DataType intRowType() {
		return DataTypes.ROW(
			DataTypes.FIELD("v", DataTypes.INT()));
	}

	private TypeInformation<RowData> intRowTypeInfo() {
		return InternalTypeInfo.of(intRowType().getLogicalType());
	}

	private DataType intRowWithTopicType() {
		return DataTypes.ROW(
			DataTypes.FIELD("v", DataTypes.INT()),
			DataTypes.FIELD(TOPIC_ATTRIBUTE_NAME, DataTypes.STRING())
		);
	}

	private TypeInformation<RowData> intRowWithTopicTypeInfo() {
		return InternalTypeInfo.of(intRowWithTopicType().getLogicalType());
	}

	private Properties sinkProperties() {
		Properties props = new Properties();
		props.setProperty(FLUSH_ON_CHECKPOINT_OPTION_KEY, "true");
		props.setProperty(FAIL_ON_WRITE_OPTION_KEY, "true");
		return props;
	}

	private Properties sourceProperties() {
		Properties props = new Properties();
		props.setProperty(PARTITION_DISCOVERY_INTERVAL_MS_OPTION_KEY, "5000");
		return props;
	}

	private DeserializationSchema<RowData> getJsonRowDataDerSchema(RowType rowType) {
		return new JsonRowDataDeserializationSchema(
			rowType,
			InternalTypeInfo.of(rowType),
			false,
			true,
			TimestampFormat.ISO_8601);
	}

	private void produceIntoPulsar(
		DataStream<RowData> stream, DataType dt, Properties props,
		TypeInformation<RowData> typeInformation) {
		props.setProperty(FLUSH_ON_CHECKPOINT_OPTION_KEY, "true");
		SerializationSchema<RowData> serializationSchema = getJsonSerializationSchema((RowType) dt.getLogicalType());
		PulsarSerializationSchema<RowData> pulsarRowSerializationSchema =
			new PulsarSerializationSchemaWrapper.Builder<>(serializationSchema)
				.useRowMode(dt, RecordSchemaType.JSON)
				.setTopicExtractor(
					(SerializableFunction<RowData, String>) row -> (String) row
						.getString(1)
						.toString())
				.build();

		stream.addSink(
			new FlinkPulsarSink<>(
				serviceUrl,
				adminUrl,
				Optional.empty(),
				props,
				pulsarRowSerializationSchema));
		//new FlinkPulsarRowSink(serviceUrl, adminUrl, Optional.empty(), props, serializationSchema, dt));
	}

	public static SerializationSchema<RowData> getJsonSerializationSchema(RowType rowType) {
		return new JsonRowDataSerializationSchema(
			rowType,
			TimestampFormat.ISO_8601,
			JsonOptions.MapNullKeyMode.DROP,
			"");
	}

	private class AssertSink extends FlinkPulsarSink<RowData> {

		private final int cacheSize;

		public AssertSink(
			String serviceUrl,
			String adminUrl,
			int cacheSize,
			Properties properties,
			DataType dataType) {
			super(
				serviceUrl,
				adminUrl,
				Optional.empty(),
				properties,
				new PulsarSerializationSchemaWrapper.Builder<>(getJsonSerializationSchema(
					(RowType) intRowWithTopicType().getLogicalType()))
					.setTopicExtractor(
						(SerializableFunction<RowData, String>) row -> (String) row
							.getString(1)
							.toString())
					.useRowMode(dataType, RecordSchemaType.JSON)
					.build()
			);

			this.cacheSize = cacheSize;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			assertEquals(CachedPulsarClient.getCacheSize(), cacheSize);
		}
	}

	private static class DeserWithMeta implements DeserializationSchema<Row> {
		private final DeserializationSchema<Row> deserializationSchema;
		private final boolean hasMetaData;
		private final DataType valueType;

		public DeserWithMeta(
			DeserializationSchema<Row> deserializationSchema, boolean hasMetaData,
			DataType valueType) {
			this.hasMetaData = hasMetaData;
			this.deserializationSchema = deserializationSchema;
			this.valueType = valueType;
		}

		@Override
		public Row deserialize(byte[] message) throws IOException {
			return deserializationSchema.deserialize(message);
		}

		@Override
		public boolean isEndOfStream(Row nextElement) {
			return deserializationSchema.isEndOfStream(nextElement);
		}

		@Override
		public TypeInformation<Row> getProducedType() {
			return getMetaProducedType(valueType, true);
		}
	}

	public static TypeInformation<Row> getMetaProducedType(
		DataType valueType,
		boolean hasMetaData) {
		List<DataTypes.Field> mainSchema = new ArrayList<>();
		if (valueType instanceof FieldsDataType) {
			FieldsDataType fieldsDataType = (FieldsDataType) valueType;
			RowType rowType = (RowType) fieldsDataType.getLogicalType();
			List<String> fieldNames = rowType.getFieldNames();
			for (int i = 0; i < fieldNames.size(); i++) {
				org.apache.flink.table.types.logical.LogicalType logicalType = rowType.getTypeAt(i);
				DataTypes.Field field =
					DataTypes.FIELD(
						fieldNames.get(i),
						TypeConversions.fromLogicalToDataType(logicalType));
				mainSchema.add(field);
			}

		} else {
			mainSchema.add(DataTypes.FIELD("value", valueType));
		}

		if (hasMetaData) {
			mainSchema.addAll(SimpleSchemaTranslator.METADATA_FIELDS);
		}
		FieldsDataType fieldsDataType = (FieldsDataType) DataTypes.ROW(mainSchema.toArray(new DataTypes.Field[0]));
		return (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(fieldsDataType);
	}

	private static class CountMessageNumberFM implements FlatMapFunction<RowData, Integer> {
		private final int numElements;
		private final int topicPos;

		private final Map<String, Integer> map;

		private CountMessageNumberFM(int numElements, int topicPos) {
			this.numElements = numElements;
			this.map = new HashMap<>();
			this.topicPos = topicPos;
		}

		@Override
		public void flatMap(RowData value, Collector<Integer> out) throws Exception {
			String topic = value.getString(topicPos).toString();
			Integer old = map.getOrDefault(topic, 0);
			map.put(topic, old + 1);

			for (Map.Entry<String, Integer> entry : map.entrySet()) {
				if (entry.getValue() < numElements) {
					return;
				} else if (entry.getValue() > numElements) {
					throw new RuntimeException(
						entry.getKey() + " has " + entry.getValue() + "elements");
				}
			}
			throw new SuccessException();
		}
	}

	private static Map<String, MessageId> getCommittedOffsets(List<String> topics, String prefix) {
		try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
			Map<String, MessageId> results = new HashMap<>();
			String subName = "flink-pulsar-" + prefix;

			for (String topic : topics) {
				int index = TopicName.get(topic).getPartitionIndex();
				MessageId mid = null;

				try {
					// In key-shared mode, filter out the largest cursor
					PersistentTopicInternalStats.CursorStats cursor = admin
						.topics()
						.getInternalStats(topic).cursors
						.entrySet()
						.stream()
						.filter(e -> e.getKey().startsWith(subName))
						.map(Map.Entry::getValue)
						.max(Comparator.comparing(a -> a.readPosition))
						.orElse(null);
					if (cursor != null) {
						String[] le = cursor.readPosition.split(":");
						long ledgerId = Long.parseLong(le[0]);
						long entryId = Long.parseLong(le[1]);
						mid = new MessageIdImpl(ledgerId, entryId, index);
						System.out.println(String.format(
							"get cursor for topicp[{}] -> {}",
							topic,
							mid.toString()));
					}
				} catch (PulsarAdminException e) {
					// do nothing
				}
				results.put(topic, mid);
			}
			return results;
		} catch (PulsarClientException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static boolean roughEquals(Map<String, MessageId> a, Map<String, MessageId> b) {
		for (Map.Entry<String, MessageId> aE : a.entrySet()) {
			MessageId bmid = b.getOrDefault(aE.getKey(), MessageId.latest);
			if (!ReaderThread.messageIdRoughEquals(bmid, aE.getValue())) {
				return false;
			}
		}
		return true;
	}

	public static void createTopic(String topic, int partition, String adminUrl) {
		assert partition > 1;
		try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
			admin.topics().createPartitionedTopic(topic, partition);
		} catch (PulsarClientException | PulsarAdminException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Check if all message exist.
	 */
	public static class CheckAllMessageExist extends RichFlatMapFunction<RowData, RowData> {
		private final Map<String, Set<Integer>> expected;
		private final int total;
		private final int topicPos;

		private final Map<String, List<Integer>> map = new HashMap<>();
		private int count = 0;

		public CheckAllMessageExist(Map<String, Set<Integer>> expected, int total, int topicPos) {
			this.expected = expected;
			this.total = total;
			this.topicPos = topicPos;
		}

		@Override
		public void flatMap(RowData value, Collector<RowData> out) throws Exception {
			String topic = value.getString(topicPos).toString();
			int v = value.getInt(0);
			List<Integer> current = map.getOrDefault(topic, new ArrayList<>());
			current.add(v);
			map.put(topic, current);

			count++;
			out.collect(value);
			if (count == total) {
				for (Map.Entry<String, List<Integer>> e : map.entrySet()) {
					Set<Integer> s = new HashSet<>(e.getValue());
					if (s.size() != e.getValue().size()) {
						throw new RuntimeException(
							"duplicate elements in " + topic + " " + e.getValue().toString());
					}
					Set<Integer> expectedSet = expected.getOrDefault(e.getKey(), null);
					if (expectedSet == null) {
						throw new RuntimeException("Unknown topic seen " + e.getKey());
					} else {
						if (!expectedSet.equals(s)) {
							throw new RuntimeException("" + expectedSet + "\n" + s);
						}
					}
				}
				throw new SuccessException();
			}
		}
	}

	private void generateRandomizedIntegerSequence(
		StreamExecutionEnvironment see,
		String tp,
		int numPartitions,
		int numElements,
		boolean randomizedOrder) throws Exception {
		see.setParallelism(numPartitions);
		see.setRestartStrategy(RestartStrategies.noRestart());

		DataStream<RowData> stream = see.addSource(new RandomizedIntegerRowSeq(
			tp,
			numPartitions,
			numElements,
			randomizedOrder));
		produceIntoPulsar(
			stream,
			intRowWithTopicType(),
			sinkProperties(),
			intRowWithTopicTypeInfo());
		see.execute("scrambles in sequence generator");
	}

	private static class PojoSeq extends RichSourceFunction<SchemaData.FA> {
		private final int numElements;
		private volatile boolean running = true;

		private PojoSeq(int numElements) {
			this.numElements = numElements;
		}

		@Override
		public void run(SourceContext<SchemaData.FA> ctx) throws Exception {
			List<SchemaData.FA> faList = SchemaData.faList;
			//List<SchemaData.Bar> list = new ArrayList<>();

			faList.forEach(r -> ctx.collect(r));
		}

		@Override
		public void cancel() {
			running = false;
		}

	}

	private static class RandomizedIntegerRowSeq extends RichParallelSourceFunction<RowData> {
		private final String tp;
		private final int numPartitions;
		private final int numElements;
		private final boolean randomizedOrder;

		private volatile boolean running = true;

		private RandomizedIntegerRowSeq(
			String tp,
			int numPartitions,
			int numElements,
			boolean randomizedOrder) {
			this.tp = tp;
			this.numPartitions = numPartitions;
			this.numElements = numElements;
			this.randomizedOrder = randomizedOrder;
		}

		@Override
		public void run(SourceContext<RowData> ctx) throws Exception {
			int subIndex = getRuntimeContext().getIndexOfThisSubtask();
			int all = getRuntimeContext().getNumberOfParallelSubtasks();

			List<RowData> rows = new ArrayList<>();
			for (int i = 0; i < numElements; i++) {
				rows.add(intRowWithTopic(subIndex + i * all, topicName(tp, subIndex)));
			}

			if (randomizedOrder) {
				Random rand = new Random();
				for (int i = 0; i < numElements; i++) {
					int otherPos = rand.nextInt(numElements);
					RowData tmp = rows.get(i);
					rows.set(i, rows.get(otherPos));
					rows.set(otherPos, tmp);
				}
			}

			rows.forEach(ctx::collect);
		}

		@Override
		public void cancel() {
			running = false;
		}

		String topicName(String tp, int index) {
			return tp + "-partition-" + index;
		}

		RowData intRowWithTopic(int i, String tp) {
			final GenericRowData rowData = new GenericRowData(RowKind.INSERT, 2);
			RowDataUtil.setField(rowData, 0, i);
			RowDataUtil.setField(rowData, 1, tp);
			return rowData;
		}
	}

	private static class PartitionValidationMapper implements MapFunction<RowData, RowData> {
		private final int numPartitions;
		private final int maxPartitions;

		private final HashSet<String> myTopics;

		private PartitionValidationMapper(int numPartitions, int maxPartitions) {
			this.numPartitions = numPartitions;
			this.maxPartitions = maxPartitions;
			myTopics = new HashSet<>();
		}

		@Override
		public RowData map(RowData value) throws Exception {
			String topic = value.getString(1).toString();
			myTopics.add(topic);
			if (myTopics.size() > maxPartitions) {
				throw new Exception(String.format(
					"Error: Elements from too many different partitions: %s, Expected elements only from %d partitions",
					myTopics.toString(),
					maxPartitions));
			}
			return value;
		}
	}

	private static class InfiniteStringGenerator extends Thread {
		private final String tp;

		private volatile boolean running = true;
		private volatile Throwable error = null;

		public InfiniteStringGenerator(String tp) {
			this.tp = tp;
		}

		@Override
		public void run() {
			try {
				Properties props = new Properties();
				props.setProperty(FLUSH_ON_CHECKPOINT_OPTION_KEY, "true");
				props.setProperty(FAIL_ON_WRITE_OPTION_KEY, "true");
				ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
				clientConfigurationData.setServiceUrl(serviceUrl);
				StreamSink<String> sink = new StreamSink<>(
					new FlinkPulsarSinkBase<String>(
						adminUrl,
						Optional.of(tp),
						clientConfigurationData,
						props,
						new PulsarSerializationSchemaWrapper.Builder<>(
							new SimpleStringSchema())
							.useSpecialMode(Schema.STRING)
							.build(),
						null) {
						@Override
						protected void invoke(
							PulsarTransactionState<String> stringPulsarTransactionState,
							String s,
							Context context) throws Exception {
							return;
						}
					});

				OneInputStreamOperatorTestHarness<String, Object> testHarness =
					new OneInputStreamOperatorTestHarness(sink);
				testHarness.open();

				StringBuilder bld = new StringBuilder();
				Random rnd = new Random();

				while (running) {
					bld.setLength(0);
					int len = rnd.nextInt(100) + 1;
					for (int i = 0; i < len; i++) {
						bld.append((char) (rnd.nextInt(20) + 'a'));
					}

					String next = bld.toString();
					testHarness.processElement(new StreamRecord<>(next));
				}
			} catch (Exception e) {
				this.error = e;
			}
		}

		public void shutdown() {
			this.running = false;
			this.interrupt();
		}

		public Throwable getError() {
			return error;
		}
	}

	private static class FlinkPulsarRowSourceSub extends FlinkPulsarSource<Integer> {

		private final String sub;

		public FlinkPulsarRowSourceSub(
			String sub,
			String serviceUrl,
			String adminUrl,
			Properties properties) {
			super(serviceUrl, adminUrl, new PulsarPrimitiveSchema<>(Integer.class), properties);
			this.sub = sub;
		}

		@Override
		protected String getSubscriptionName() {
			return "flink-pulsar-" + sub;
		}
	}

	/**
	 * add topic field for RowData pos 2.
	 */
	public static class RowDataPulsarDeserializationSchema implements PulsarDeserializationSchema<RowData> {

		private final AtomicDataType dataType;

		private volatile DeserializationSchema<RowData> deserializationSchema;

		public RowDataPulsarDeserializationSchema(
			AtomicDataType dataType) {
			this.dataType = dataType;
		}

		@Override
		public void open(DeserializationSchema.InitializationContext context) throws Exception {
			deserializationSchema =
				new AtomicRowDataDeserializationSchema(dataType
					.getConversionClass()
					.getCanonicalName(), false);
		}

		@Override
		public RowData deserialize(Message<RowData> message) throws IOException {
			final GenericRowData value = (GenericRowData) message.getValue();
			final Object field = RowDataUtil.getField(value, 0, dataType.getConversionClass());

			GenericRowData result = new GenericRowData(RowKind.INSERT, 2);
			RowDataUtil.setField(result, 0, field);
			RowDataUtil.setField(result, 1, message.getTopicName());
			return result;
		}

		@Override
		public TypeInformation<RowData> getProducedType() {
			return InternalTypeInfo.of(DataTypes.ROW(
				DataTypes.FIELD("v", dataType),
				DataTypes.FIELD(TOPIC_ATTRIBUTE_NAME, DataTypes.STRING())
			).getLogicalType());
		}

		@Override
		public Schema<RowData> getSchema() {
			try {
				Schema pulsarSchema = SimpleSchemaTranslator.atomicType2PulsarSchema(dataType);
				return new FlinkSchema<>(pulsarSchema.getSchemaInfo(), null, deserializationSchema);
			} catch (IncompatibleSchemaException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public boolean isEndOfStream(RowData nextElement) {
			return false;
		}
	}
}
