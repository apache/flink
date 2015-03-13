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

package org.apache.flink.streaming.connectors.kafka.config;

import java.io.Serializable;
import java.util.Properties;

import kafka.utils.VerifiableProperties;

/**
 * Wraps an arbitrary Serializable (e.g. a Partitioner) object to use in Kafka, to shade the properties.
 *
 * @param <T>
 * 		Type of the object to wrap
 */
public abstract class KafkaConfigWrapper<T extends Serializable> {

	private StringSerializer<T> stringSerializer;

	protected T wrapped;

	public KafkaConfigWrapper(T wrapped) {
		this();
		this.wrapped = wrapped;
	}

	private KafkaConfigWrapper() {
		stringSerializer = new StringSerializer<T>();
	}

	public KafkaConfigWrapper(VerifiableProperties properties) {
		this();
		read(properties);
	}

	public void read(VerifiableProperties properties) {
		String stringWrapped = properties.getString(getClass().getCanonicalName());
		wrapped = stringSerializer.deserialize(stringWrapped);
	}

	public void write(Properties properties) {
		properties.put(getClass().getCanonicalName(), stringSerializer.serialize(wrapped));
	}

}
