/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.connectors.kafka.v2.sink;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.connectors.kafka.v2.KafkaBaseOutputFormat;
import org.apache.flink.streaming.connectors.kafka.v2.KafkaConverter;

import java.util.Properties;

/** Kafka09 OutputFormat. **/
public class Kafka09OutputFormat extends KafkaBaseOutputFormat {
	public Kafka09OutputFormat(
			String defaultTopicId,
			KafkaConverter serializationSchema,
			Properties producerConfig) {
		super(defaultTopicId, serializationSchema, producerConfig);
	}

	@Override
	protected void flush() {
		if (this.producer != null) {
			producer.flush();
		}
	}

	/** Builder for Kafka09 OutputFormat. **/
	public static class Builder {
		private RowTypeInfo rowTypeInfo;
		private String topic;
		private KafkaConverter kafkaConverter;
		private Properties properties;

		public Builder setTopic(String topic) {
			this.topic = topic;
			return this;
		}

		public Builder setKafkaConverter(KafkaConverter kafkaConverter) {
			this.kafkaConverter = kafkaConverter;
			return this;
		}

		public Builder setProperties(Properties properties) {
			this.properties = properties;
			return this;
		}

		public Builder setRowTypeInfo(RowTypeInfo rowTypeInfo) {
			this.rowTypeInfo = rowTypeInfo;
			return this;
		}

		public Kafka09OutputFormat build() {
			return new Kafka09OutputFormat(topic, kafkaConverter, properties);
		}
	}
}
