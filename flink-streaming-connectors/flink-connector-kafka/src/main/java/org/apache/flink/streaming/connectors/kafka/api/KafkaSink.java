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
package org.apache.flink.streaming.connectors.kafka.api;


import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 * Sink that emits its inputs to a Kafka topic.
 *
 * The KafkaSink has been relocated to org.apache.flink.streaming.connectors.kafka.KafkaSink.
 * This class will be removed in future releases of Flink.
 * 
 * @deprecated Please use the {@link org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer} instead.
 */
@Deprecated
public class KafkaSink<IN> extends FlinkKafkaProducer<IN> {
	public KafkaSink(String brokerList, String topicId, SerializationSchema<IN> serializationSchema) {
		super(brokerList, topicId, serializationSchema);
	}
}
