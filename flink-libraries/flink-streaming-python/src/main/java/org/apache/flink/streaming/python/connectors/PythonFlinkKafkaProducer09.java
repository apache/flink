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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.python.util.serialization.PythonSerializationSchema;

import org.python.core.PyObject;

import java.io.IOException;

/**
 * Defines a generic Python implementation of {@link FlinkKafkaProducer09}, by using a type
 * parameter of {@code PyObject} class. It can be used as a sink in the python code.
 */
public class PythonFlinkKafkaProducer09 extends FlinkKafkaProducer09<PyObject> {

	public PythonFlinkKafkaProducer09(String brokerList, String topicId, SerializationSchema<PyObject> serializationSchema) throws IOException {
		super(brokerList, topicId, new PythonSerializationSchema(serializationSchema));
	}

	public void set_log_failures_only(boolean logFailuresOnly) {
		this.setLogFailuresOnly(logFailuresOnly);
	}

	public void set_flush_on_checkpoint(boolean flush) {
		this.setFlushOnCheckpoint(flush);
	}
}
