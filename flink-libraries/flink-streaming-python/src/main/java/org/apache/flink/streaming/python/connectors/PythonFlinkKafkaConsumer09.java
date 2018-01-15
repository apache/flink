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

package org.apache.flink.streaming.python.connectors;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.python.util.serialization.PythonDeserializationSchema;

import java.io.IOException;
import java.util.Properties;

/**
 * Defines a generic implementation of the {@link FlinkKafkaConsumer09} class, by using a type
 * parameter of an {@code Object} class. The thin Python streaming layer converts the elements
 * that are read from the Kafka feed, to a {@code PyObject} for further handling.
 */
public class PythonFlinkKafkaConsumer09 extends FlinkKafkaConsumer09<Object> {

	public PythonFlinkKafkaConsumer09(String topic, DeserializationSchema<Object> schema, Properties props) throws IOException {
		super(topic, new PythonDeserializationSchema(schema), props);
	}
}
