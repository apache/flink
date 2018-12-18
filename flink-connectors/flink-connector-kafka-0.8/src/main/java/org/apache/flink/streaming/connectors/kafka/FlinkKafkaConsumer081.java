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

import org.apache.flink.api.common.serialization.DeserializationSchema;

import java.util.Properties;

/**
 * THIS CLASS IS DEPRECATED. Use FlinkKafkaConsumer08 instead.
 *
 * @deprecated Use {@link FlinkKafkaConsumer08}
 */
@Deprecated
public class FlinkKafkaConsumer081<T> extends FlinkKafkaConsumer08<T> {

	private static final long serialVersionUID = -5649906773771949146L;

	/**
	 * THIS CONSTRUCTOR IS DEPRECATED. Use FlinkKafkaConsumer08 instead.
	 *
	 * @deprecated Use {@link FlinkKafkaConsumer08#FlinkKafkaConsumer08(String, DeserializationSchema, Properties)}
	 */
	@Deprecated
	public FlinkKafkaConsumer081(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
		super(topic, valueDeserializer, props);
	}
}

