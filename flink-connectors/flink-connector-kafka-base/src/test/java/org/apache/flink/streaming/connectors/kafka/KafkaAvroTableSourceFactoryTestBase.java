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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.TestTableSourceDescriptor;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceFactoryService;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Tests for {@link KafkaAvroTableSourceFactory}.
 */
public abstract class KafkaAvroTableSourceFactoryTestBase {

	private static final String TOPIC = "test-topic";

	protected abstract String version();

	protected abstract KafkaAvroTableSource.Builder builder();

	@Test
	public void testTableSourceFromAvroSchema() {
		testTableSource(new Avro().recordClass(Address.class));
	}

	private void testTableSource(FormatDescriptor format) {
		// construct table source using a builder

		final Map<String, String> tableAvroMapping = new HashMap<>();
		tableAvroMapping.put("a_street", "street");
		tableAvroMapping.put("street", "street");
		tableAvroMapping.put("b_city", "city");
		tableAvroMapping.put("city", "city");
		tableAvroMapping.put("c_state", "state");
		tableAvroMapping.put("state", "state");
		tableAvroMapping.put("zip", "zip");
		tableAvroMapping.put("num", "num");

		final Properties props = new Properties();
		props.put("group.id", "test-group");
		props.put("bootstrap.servers", "localhost:1234");

		final Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();
		specificOffsets.put(new KafkaTopicPartition(TOPIC, 0), 100L);
		specificOffsets.put(new KafkaTopicPartition(TOPIC, 1), 123L);

		KafkaAvroTableSource.Builder builder = builder();

		builder.forAvroRecordClass(Address.class)
				.withTableToAvroMapping(tableAvroMapping);

		final KafkaTableSource builderSource = builder
				.withKafkaProperties(props)
				.forTopic(TOPIC)
				.fromSpecificOffsets(specificOffsets)
				.withSchema(
						TableSchema.builder()
								.field("a_street", Types.STRING)
								.field("b_city", Types.STRING)
								.field("c_state", Types.STRING)
								.field("zip", Types.STRING)
								.field("proctime", Types.SQL_TIMESTAMP)
								.build())
				.withProctimeAttribute("proctime")
				.build();

		// construct table source using descriptors and table source factory

		final Map<Integer, Long> offsets = new HashMap<>();
		offsets.put(0, 100L);
		offsets.put(1, 123L);

		final TestTableSourceDescriptor testDesc = new TestTableSourceDescriptor(
				new Kafka()
						.version(version())
						.topic(TOPIC)
						.properties(props)
						.startFromSpecificOffsets(offsets))
				.addFormat(format)
				.addSchema(
						new Schema()
								.field("a_street", Types.STRING).from("street")
								.field("b_city", Types.STRING).from("city")
								.field("c_state", Types.STRING).from("state")
								.field("zip", Types.STRING)
								.field("proctime", Types.SQL_TIMESTAMP).proctime());

		final TableSource<?> factorySource = TableSourceFactoryService.findAndCreateTableSource(testDesc);

		assertEquals(builderSource, factorySource);
	}
}
