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

package org.apache.flink.streaming.examples.windowing.clickeventcount.records;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * A Kafka {@link KafkaSerializationSchema} to serialize {@link ClickEventStatistics}s as JSON.
 *
 */
public class ClickEventStatisticsSerializationSchema implements KafkaSerializationSchema<ClickEventStatistics> {

	private static final ObjectMapper objectMapper = new ObjectMapper();
	private String topic;

	public ClickEventStatisticsSerializationSchema(){
	}

	public ClickEventStatisticsSerializationSchema(String topic) {
		this.topic = topic;
	}

	@Override
	public ProducerRecord<byte[], byte[]> serialize(
			final ClickEventStatistics message, @Nullable final Long timestamp) {
		try {
			//if topic is null, default topic will be used
			return new ProducerRecord<>(topic, objectMapper.writeValueAsBytes(message));
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException("Could not serialize record: " + message, e);
		}
	}
}
