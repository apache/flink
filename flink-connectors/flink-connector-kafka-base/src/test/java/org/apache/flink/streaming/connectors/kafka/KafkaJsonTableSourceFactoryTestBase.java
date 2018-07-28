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
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.TestTableDescriptor;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceUtil;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Tests for legacy KafkaJsonTableSourceFactory.
 *
 * @deprecated Ensures backwards compatibility with Flink 1.5. Can be removed once we
 *             drop support for format-specific table sources.
 */
@Deprecated
public abstract class KafkaJsonTableSourceFactoryTestBase {

	private static final String JSON_SCHEMA =
		"{" +
		"  'title': 'Fruit'," +
		"  'type': 'object'," +
		"  'properties': {" +
		"    'name': {" +
		"      'type': 'string'" +
		"    }," +
		"    'count': {" +
		"      'type': 'integer'" +
		"    }," +
		"    'time': {" +
		"      'description': 'row time'," +
		"      'type': 'string'," +
		"      'format': 'date-time'" +
		"    }" +
		"  }," +
		"  'required': ['name', 'count', 'time']" +
		"}";

	private static final String TOPIC = "test-topic";

	protected abstract String version();

	protected abstract KafkaJsonTableSource.Builder builder();

	@Test
	public void testTableSourceFromJsonSchema() {
		testTableSource(
			new Json()
				.jsonSchema(JSON_SCHEMA)
				.failOnMissingField(true)
		);
	}

	@Test
	public void testTableSourceDerivedSchema() {
		testTableSource(
			new Json()
				.deriveSchema()
				.failOnMissingField(true)
		);
	}

	private void testTableSource(FormatDescriptor format) {
		// construct table source using a builder

		final Map<String, String> tableJsonMapping = new HashMap<>();
		tableJsonMapping.put("fruit-name", "name");
		tableJsonMapping.put("name", "name");
		tableJsonMapping.put("count", "count");
		tableJsonMapping.put("time", "time");

		final Properties props = new Properties();
		props.put("group.id", "test-group");
		props.put("bootstrap.servers", "localhost:1234");

		final Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();
		specificOffsets.put(new KafkaTopicPartition(TOPIC, 0), 100L);
		specificOffsets.put(new KafkaTopicPartition(TOPIC, 1), 123L);

		final KafkaTableSource builderSource = builder()
				.forJsonSchema(TableSchema.fromTypeInfo(JsonRowSchemaConverter.convert(JSON_SCHEMA)))
				.failOnMissingField(true)
				.withTableToJsonMapping(tableJsonMapping)
				.withKafkaProperties(props)
				.forTopic(TOPIC)
				.fromSpecificOffsets(specificOffsets)
				.withSchema(
					TableSchema.builder()
						.field("fruit-name", Types.STRING)
						.field("count", Types.BIG_DEC)
						.field("event-time", Types.SQL_TIMESTAMP)
						.field("proc-time", Types.SQL_TIMESTAMP)
						.build())
				.withProctimeAttribute("proc-time")
				.withRowtimeAttribute("event-time", new ExistingField("time"), new AscendingTimestamps())
				.build();

		TableSourceUtil.validateTableSource(builderSource);

		// construct table source using descriptors and table source factory

		final Map<Integer, Long> offsets = new HashMap<>();
		offsets.put(0, 100L);
		offsets.put(1, 123L);

		final TestTableDescriptor testDesc = new TestTableDescriptor(
				new Kafka()
					.version(version())
					.topic(TOPIC)
					.properties(props)
					.startFromSpecificOffsets(offsets))
			.withFormat(format)
			.withSchema(
				new Schema()
						.field("fruit-name", Types.STRING).from("name")
						.field("count", Types.BIG_DEC) // no from so it must match with the input
						.field("event-time", Types.SQL_TIMESTAMP).rowtime(
							new Rowtime().timestampsFromField("time").watermarksPeriodicAscending())
						.field("proc-time", Types.SQL_TIMESTAMP).proctime())
			.inAppendMode();

		DescriptorProperties properties = new DescriptorProperties(true);
		testDesc.addProperties(properties);
		final TableSource<?> factorySource =
				TableFactoryService.find(StreamTableSourceFactory.class, testDesc)
						.createStreamTableSource(properties.asMap());

		assertEquals(builderSource, factorySource);
	}
}
