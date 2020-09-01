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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSink;
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSource;
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSourceSinkFactory;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for {@link Kafka010TableSource} and {@link Kafka010TableSink} created
 * by {@link Kafka010TableSourceSinkFactory}.
 */
public class Kafka010DynamicTableFactoryTest extends KafkaDynamicTableFactoryTestBase {
	@Override
	protected String factoryIdentifier() {
		return Kafka010DynamicTableFactory.IDENTIFIER;
	}

	@Override
	protected Class<?> getExpectedConsumerClass() {
		return FlinkKafkaConsumer010.class;
	}

	@Override
	protected Class<?> getExpectedProducerClass() {
		return FlinkKafkaProducer010.class;
	}

	@Override
	protected KafkaDynamicSourceBase getExpectedScanSource(
			DataType producedDataType,
			@Nullable List<String> topics,
			@Nullable Pattern topicPattern,
			Properties properties,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			long startupTimestamp) {
		return new Kafka010DynamicSource(
				producedDataType,
				topics,
				topicPattern,
				properties,
				decodingFormat,
				startupMode,
				specificStartupOffsets,
				startupTimestamp);
	}

	@Override
	protected KafkaDynamicSinkBase getExpectedSink(
			DataType consumedDataType,
			String topic,
			Properties properties,
			Optional<FlinkKafkaPartitioner<RowData>> partitioner,
			EncodingFormat<SerializationSchema<RowData>> encodingFormat,
			KafkaSinkSemantic semantic) {
		// we only support "at-least-once" for kafka 0.10 sink.
		// if users use `sink.semantic` for kafka 0.10, an exception should be thrown
		return new Kafka010DynamicSink(
				consumedDataType,
				topic,
				properties,
				partitioner,
				encodingFormat);
	}

	@Override
	protected Map<String, String> getFullSinkOptions(){
		Map<String, String> options = super.getFullSinkOptions();
		// remove 'sink.semantic' as kafka 0.10 doesn't support it
		options.remove("sink.semantic");
		return options;
	}

	@Override
	public void testInvalidSinkSemantic() {
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
			"default",
			"default",
			"sinkTable");

		final Map<String, String> modifiedOptions = getModifiedOptions(
			getFullSinkOptions(),
			options -> {
				options.put("sink.semantic", "exactly-once");
			});
		final CatalogTable sinkTable = createKafkaSinkCatalogTable(modifiedOptions);

		try {
			FactoryUtil.createTableSink(
				null,
				objectIdentifier,
				sinkTable,
				new Configuration(),
				Thread.currentThread().getContextClassLoader());
			fail("Connector 'kafka-0.10' doesn't support 'sink.semantic'");
		} catch (Exception e) {
			assertTrue(ExceptionUtils
				.findThrowableWithMessage(e, "Unsupported options:\n\nsink.semantic")
				.isPresent());
		}
	}
}
