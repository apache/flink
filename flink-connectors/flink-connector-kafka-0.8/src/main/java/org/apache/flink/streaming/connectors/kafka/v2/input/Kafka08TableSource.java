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

package org.apache.flink.streaming.connectors.kafka.v2.input;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.v2.KafkaBaseTableSource;
import org.apache.flink.streaming.connectors.kafka.v2.KafkaMessageDeserialization;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.StringUtils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/** TableSource for Kafka08. **/
public class Kafka08TableSource extends KafkaBaseTableSource {
	public Kafka08TableSource(
			List<String> topic,
			String topicPattern,
			Properties properties,
			StartupMode startupMode,
			long startTimeStamp,
			boolean isFinite,
			BaseRowTypeInfo baseRowTypeInfo) {
		super(topic, topicPattern, properties, startupMode, startTimeStamp, isFinite, baseRowTypeInfo);
	}

	@Override
	public FlinkKafkaConsumerBase createKafkaConsumer() {
		FlinkKafkaConsumerBase consumer;
		KafkaMessageDeserialization kafkaMessageDeserialization = new KafkaMessageDeserialization(baseRowTypeInfo);
		Pattern pattern;
		if (!StringUtils.isNullOrWhitespaceOnly(topicPattern)) {
			pattern = Pattern.compile(topicPattern);
			consumer = new FlinkKafkaConsumer08(pattern, kafkaMessageDeserialization, properties);
		} else {
			consumer = new FlinkKafkaConsumer08(topic, kafkaMessageDeserialization, properties);
		}
		return consumer;
	}

	@Override
	public int getTopicPartitionSize() {
		Properties props = new Properties(properties);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		KafkaProducer producer = createKafkaProducer(props);
		try {
			int size = 0;
			for (String t : topic) {
				size += producer.partitionsFor(t).size();
			}
			return size;
		} finally {
			producer.close();
		}
	}

	// load KafkaProducer via system classloader instead of application classloader, otherwise we will hit classloading
	// issue in scala-shell scenario where kafka jar is not shipped with JobGraph, but shipped when starting yarn
	// container.
	private KafkaProducer createKafkaProducer(Properties kafkaProperties) {
		try {
			return new KafkaProducer<>(kafkaProperties);
		} catch (KafkaException e) {
			ClassLoader original = Thread.currentThread().getContextClassLoader();
			try {
				Thread.currentThread().setContextClassLoader(null);
				return new KafkaProducer<>(kafkaProperties);
			} finally {
				Thread.currentThread().setContextClassLoader(original);
			}
		}
	}
}
