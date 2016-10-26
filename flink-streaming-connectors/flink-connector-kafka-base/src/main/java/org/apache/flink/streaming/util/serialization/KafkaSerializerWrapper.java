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

package org.apache.flink.streaming.util.serialization;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.InstantiationUtil;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.Objects;

/**
 * Wrapper for using a {@see org.apache.kafka.common.serialization.Serializer} with Apache Flink.
 */
public class KafkaSerializerWrapper<K, V> implements KeyedSerializationSchema<Tuple2<K,V>> {

	private final Class<Serializer<K>> keySerializerClass;
	private final Class<Serializer<V>> valueSerializerClass;
	private Map<String, ?> configs;
	private boolean isInitialized = false;
	private Serializer<K> keySerializer;
	private Serializer<V> valueSerializer;

	// Kafka's Serializer.serialize(topic, message) method expects a topic-argument.
	// Flink's serialize*() methods don't provide the topic.
	// Some Kafka serializers expect the topic to be set. Therefore, there is a special
	// configuration key to set the topic for the serializer through this wrapper.
	private String topic = null;
	public final static String SERIALIZER_TOPIC = "__FLINK_SERIALIZER_TOPIC";

	@SuppressWarnings("unchecked")
	public KafkaSerializerWrapper(Class keySerializerClass, Class valueSerializerClass) {
		InstantiationUtil.checkForInstantiation(keySerializerClass);
		InstantiationUtil.checkForInstantiation(valueSerializerClass);
		if(!Serializer.class.isAssignableFrom(keySerializerClass)) {
			throw new IllegalArgumentException("Key serializer is not implementing the org.apache.kafka.common.serialization.Serializer interface");
		}
		if(!Serializer.class.isAssignableFrom(valueSerializerClass)) {
			throw new IllegalArgumentException("Value serializer is not implementing the org.apache.kafka.common.serialization.Serializer interface");
		}

		this.keySerializerClass = Objects.requireNonNull(keySerializerClass, "Key serializer is null");
		this.valueSerializerClass = Objects.requireNonNull(valueSerializerClass, "Value serializer is null");
	}

	public KafkaSerializerWrapper(Class keySerializerClass, Class valueSerializerClass, Map<String, ?> configs) {
		this(keySerializerClass, valueSerializerClass);
		this.configs = configs;
		Object topic = configs.get(SERIALIZER_TOPIC);
		if(topic != null) {
			if(!(topic instanceof String)) {
				throw new IllegalArgumentException("The provided topic is not of type String");
			}
			this.topic = (String) topic;
		}
	}

	private void initialize() {
		keySerializer = InstantiationUtil.instantiate(keySerializerClass, Serializer.class);
		keySerializer.configure(configs, true);
		valueSerializer = InstantiationUtil.instantiate(valueSerializerClass, Serializer.class);
		valueSerializer.configure(configs, false);
		isInitialized = true;
	}

	@Override
	public byte[] serializeKey(Tuple2<K, V> element) {
		if(!isInitialized) {
			initialize();
		}
		return keySerializer.serialize(this.topic, element.f0);
	}

	@Override
	public byte[] serializeValue(Tuple2<K, V> element) {
		if(!isInitialized) {
			initialize();
		}
		return valueSerializer.serialize(this.topic, element.f1);
	}

	@Override
	public String getTargetTopic(Tuple2<K, V> element) {
		return null;
	}
}
