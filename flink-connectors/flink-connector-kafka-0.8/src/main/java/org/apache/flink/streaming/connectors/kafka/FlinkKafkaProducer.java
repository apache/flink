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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import java.util.Properties;

/**
 * THIS CLASS IS DEPRECATED. Use FlinkKafkaProducer08 instead.
 *
 * @deprecated Use {@link FlinkKafkaProducer08}.
 */
@Deprecated
public class FlinkKafkaProducer<IN> extends FlinkKafkaProducer08<IN>  {

	/**
	 * @deprecated Use {@link FlinkKafkaProducer08#FlinkKafkaProducer08(String, String, SerializationSchema)}
	 */
	@Deprecated
	public FlinkKafkaProducer(String brokerList, String topicId, SerializationSchema<IN> serializationSchema) {
		super(topicId, new KeyedSerializationSchemaWrapper<>(serializationSchema), getPropertiesFromBrokerList(brokerList), (FlinkKafkaPartitioner<IN>) null);
	}

	/**
	 * @deprecated Use {@link FlinkKafkaProducer08#FlinkKafkaProducer08(String, SerializationSchema, Properties)}
	 */
	@Deprecated
	public FlinkKafkaProducer(String topicId, SerializationSchema<IN> serializationSchema, Properties producerConfig) {
		super(topicId, new KeyedSerializationSchemaWrapper<>(serializationSchema), producerConfig, (FlinkKafkaPartitioner<IN>) null);
	}

	/**
	 * @deprecated Use {@link FlinkKafkaProducer08#FlinkKafkaProducer08(String, SerializationSchema, Properties, KafkaPartitioner)}
	 */
	@Deprecated
	public FlinkKafkaProducer(String topicId, SerializationSchema<IN> serializationSchema, Properties producerConfig, KafkaPartitioner customPartitioner) {
		super(topicId, new KeyedSerializationSchemaWrapper<>(serializationSchema), producerConfig, customPartitioner);

	}

	/**
	 * @deprecated Use {@link FlinkKafkaProducer08#FlinkKafkaProducer08(String, String, KeyedSerializationSchema)}
	 */
	@Deprecated
	public FlinkKafkaProducer(String brokerList, String topicId, KeyedSerializationSchema<IN> serializationSchema) {
		super(topicId, serializationSchema, getPropertiesFromBrokerList(brokerList), (FlinkKafkaPartitioner<IN>) null);
	}

	/**
	 * @deprecated Use {@link FlinkKafkaProducer08#FlinkKafkaProducer08(String, KeyedSerializationSchema, Properties)}
	 */
	@Deprecated
	public FlinkKafkaProducer(String topicId, KeyedSerializationSchema<IN> serializationSchema, Properties producerConfig) {
		super(topicId, serializationSchema, producerConfig, (FlinkKafkaPartitioner<IN>) null);
	}

	/**
	 * @deprecated Use {@link FlinkKafkaProducer08#FlinkKafkaProducer08(String, KeyedSerializationSchema, Properties, KafkaPartitioner)}
	 */
	@Deprecated
	public FlinkKafkaProducer(String topicId, KeyedSerializationSchema<IN> serializationSchema, Properties producerConfig, KafkaPartitioner customPartitioner) {
		super(topicId, serializationSchema, producerConfig, customPartitioner);
	}

}
