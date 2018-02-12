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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;

import org.mockito.Mockito;

/**
 * Tests for {@link KafkaJsonTableSourceFactory}.
 */
public abstract class KafkaJsonTableFromDescriptorTestBase {
	private static final String GROUP_ID = "test-group";
	private static final String BOOTSTRAP_SERVERS = "localhost:1234";
	private static final String TOPIC = "test-topic";

	protected abstract String versionForTest();

	protected abstract KafkaJsonTableSource.Builder builderForTest();

	protected abstract void extraSettings(KafkaTableSource.Builder builder, Kafka kafka);

	private static StreamExecutionEnvironment env = Mockito.mock(StreamExecutionEnvironment.class);
	private static StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

//	@Test
//	public void buildJsonTableSourceTest() throws Exception {
//		final URL url = getClass().getClassLoader().getResource("kafka-json-schema.json");
//		Objects.requireNonNull(url);
//		final String schema = FileUtils.readFileUtf8(new File(url.getFile()));
//
//		Map<String, String> tableJsonMapping = new HashMap<>();
//		tableJsonMapping.put("fruit-name", "name");
//		tableJsonMapping.put("fruit-count", "count");
//		tableJsonMapping.put("event-time", "time");
//
//		// Construct with the builder.
//		Properties props = new Properties();
//		props.put("group.id", GROUP_ID);
//		props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
//
//		Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();
//		specificOffsets.put(new KafkaTopicPartition(TOPIC, 0), 100L);
//		specificOffsets.put(new KafkaTopicPartition(TOPIC, 1), 123L);
//
//		KafkaTableSource.Builder builder = builderForTest()
//				.forJsonSchema(TableSchema.fromTypeInfo(JsonSchemaConverter.convert(schema)))
//				.failOnMissingField(true)
//				.withTableToJsonMapping(tableJsonMapping)
//				.withKafkaProperties(props)
//				.forTopic(TOPIC)
//				.fromSpecificOffsets(specificOffsets)
//				.withSchema(
//						TableSchema.builder()
//								.field("fruit-name", Types.STRING)
//								.field("fruit-count", Types.INT)
//								.field("event-time", Types.LONG)
//								.field("proc-time", Types.SQL_TIMESTAMP)
//								.build())
//				.withProctimeAttribute("proc-time");
//
//		// Construct with the descriptor.
//		Map<Integer, Long> offsets = new HashMap<>();
//		offsets.put(0, 100L);
//		offsets.put(1, 123L);
//		Kafka kafka = new Kafka()
//				.version(versionForTest())
//				.groupId(GROUP_ID)
//				.bootstrapServers(BOOTSTRAP_SERVERS)
//				.topic(TOPIC)
//				.startupMode(StartupMode.SPECIFIC_OFFSETS)
//				.specificOffsets(offsets)
//				.tableJsonMapping(tableJsonMapping);
//		extraSettings(builder, kafka);
//
//		TableSource source = tEnv
//				.from(kafka)
//				.withFormat(
//						new Json()
//								.schema(schema)
//								.failOnMissingField(true))
//				.withSchema(new Schema()
//						.field("fruit-name", Types.STRING)
//						.field("fruit-count", Types.INT)
//						.field("event-time", Types.LONG)
//						.field("proc-time", Types.SQL_TIMESTAMP).proctime())
//				.toTableSource();
//
//		Assert.assertEquals(builder.build(), source);
//	}

//	@Test(expected = TableException.class)
//	public void buildJsonTableSourceFailTest() {
//		tEnv.from(
//				new Kafka()
//						.version(versionForTest())
//						.groupId(GROUP_ID)
//						.bootstrapServers(BOOTSTRAP_SERVERS)
//						.topic(TOPIC)
//						.startupMode(StartupMode.SPECIFIC_OFFSETS)
//						.specificOffsets(new HashMap<>()))
//				.withFormat(
//						new Json()
//								.schema("")
//								.failOnMissingField(true))
//				.toTableSource();
//	}
}
