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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.TestTableSourceDescriptor;
import org.apache.flink.table.formats.utils.TestDeserializationSchema;
import org.apache.flink.table.formats.utils.TestTableFormat;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceFactoryService;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Abstract test base for {@link KafkaTableSourceFactory}.
 */
public abstract class KafkaTableSourceFactoryTestBase extends TestLogger {

	@Test
	@SuppressWarnings("unchecked")
	public void testTableSource() {

		// prepare parameters for Kafka table source

		final TableSchema schema = TableSchema.builder()
			.field("fruit-name", Types.STRING())
			.field("count", Types.DECIMAL())
			.field("event-time", Types.SQL_TIMESTAMP())
			.field("proc-time", Types.SQL_TIMESTAMP())
			.build();

		final String proctimeAttribute = "proc-time";

		final List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors = Collections.singletonList(
			new RowtimeAttributeDescriptor("event-time", new ExistingField("time"), new AscendingTimestamps()));

		final Map<String, String> fieldMapping = new HashMap<>();
		fieldMapping.put("fruit-name", "name");
		fieldMapping.put("count", "count");

		final String topic = "myTopic";

		final Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("zookeeper.connect", "dummy");
		kafkaProperties.setProperty("group.id", "dummy");

		final Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();
		specificOffsets.put(new KafkaTopicPartition(topic, 0), 100L);
		specificOffsets.put(new KafkaTopicPartition(topic, 1), 123L);

		final TestDeserializationSchema deserializationSchema = new TestDeserializationSchema(
			TableSchema.builder()
				.field("name", Types.STRING())
				.field("count", Types.DECIMAL())
				.field("time", Types.SQL_TIMESTAMP())
				.build()
				.toRowType()
		);

		final StartupMode startupMode = StartupMode.SPECIFIC_OFFSETS;

		final KafkaTableSource expected = getExpectedKafkaTableSource(
			schema,
			proctimeAttribute,
			rowtimeAttributeDescriptors,
			fieldMapping,
			topic,
			kafkaProperties,
			deserializationSchema,
			startupMode,
			specificOffsets);

		// construct table source using descriptors and table source factory

		final Map<Integer, Long> offsets = new HashMap<>();
		offsets.put(0, 100L);
		offsets.put(1, 123L);

		final TestTableSourceDescriptor testDesc = new TestTableSourceDescriptor(
				new Kafka()
					.version(getKafkaVersion())
					.topic(topic)
					.properties(kafkaProperties)
					.startFromSpecificOffsets(offsets))
			.addFormat(new TestTableFormat())
			.addSchema(
				new Schema()
					.field("fruit-name", Types.STRING()).from("name")
					.field("count", Types.DECIMAL()) // no from so it must match with the input
					.field("event-time", Types.SQL_TIMESTAMP()).rowtime(
						new Rowtime().timestampsFromField("time").watermarksPeriodicAscending())
					.field("proc-time", Types.SQL_TIMESTAMP()).proctime());

		final TableSource<?> actualSource = TableSourceFactoryService.findAndCreateTableSource(testDesc);

		assertEquals(expected, actualSource);

		// test Kafka consumer
		final KafkaTableSource actualKafkaSource = (KafkaTableSource) actualSource;
		final StreamExecutionEnvironmentMock mock = new StreamExecutionEnvironmentMock();
		actualKafkaSource.getDataStream(mock);
		assertTrue(getExpectedFlinkKafkaConsumer().isAssignableFrom(mock.function.getClass()));
	}

	private static class StreamExecutionEnvironmentMock extends StreamExecutionEnvironment {

		public SourceFunction<?> function;

		@Override
		public <OUT> DataStreamSource<OUT> addSource(SourceFunction<OUT> function) {
			this.function = function;
			return super.addSource(function);
		}

		@Override
		public JobExecutionResult execute(String jobName) {
			throw new UnsupportedOperationException();
		}
	}

	// --------------------------------------------------------------------------------------------
	// For version-specific tests
	// --------------------------------------------------------------------------------------------

	protected abstract String getKafkaVersion();

	protected abstract Class<FlinkKafkaConsumerBase<Row>> getExpectedFlinkKafkaConsumer();

	protected abstract KafkaTableSource getExpectedKafkaTableSource(
		TableSchema schema,
		String proctimeAttribute,
		List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
		Map<String, String> fieldMapping,
		String topic, Properties properties,
		DeserializationSchema<Row> deserializationSchema,
		StartupMode startupMode,
		Map<KafkaTopicPartition, Long> specificStartupOffsets);
}
