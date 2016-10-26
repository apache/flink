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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.InstantiationUtil;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Wrapper for using a {@see org.apache.kafka.common.serialization.Deserializer} with Apache Flink.
 */
public class KafkaDeserializerWrapper<K, V> implements KeyedDeserializationSchema<Tuple2<K,V>> {

	private final Class<Deserializer<K>> keyDeserializerClass;
	private final Class<Deserializer<V>> valueDeserializerClass;
	private final TupleTypeInfo<Tuple2<K,V>> producedType;
	private Map<String, ?> configs;
	private boolean isInitialized = false;
	private Deserializer<K> keyDeserializer;
	private Deserializer<V> valueDeserializer;

	@SuppressWarnings("unchecked")
	public KafkaDeserializerWrapper(Class keyDeserializerClass, Class valueDeserializerClass, Class<K> keyType, Class<V> valueType) {
		InstantiationUtil.checkForInstantiation(keyDeserializerClass);
		InstantiationUtil.checkForInstantiation(valueDeserializerClass);
		if(!Deserializer.class.isAssignableFrom(keyDeserializerClass)) {
			throw new IllegalArgumentException("Key serializer is not implementing the org.apache.kafka.common.serialization.Deserializer interface");
		}
		if(!Deserializer.class.isAssignableFrom(valueDeserializerClass)) {
			throw new IllegalArgumentException("Value serializer is not implementing the org.apache.kafka.common.serialization.Deserializer interface");
		}

		this.keyDeserializerClass = Objects.requireNonNull(keyDeserializerClass, "Key deserializer is null");
		this.valueDeserializerClass = Objects.requireNonNull(valueDeserializerClass, "Value deserializer is null");
		this.producedType = new TupleTypeInfo<>(TypeExtractor.getForClass(keyType), TypeExtractor.getForClass(valueType));
	}

	public KafkaDeserializerWrapper(Class keyDeserializerClass, Class valueDeserializerClass, Class<K> keyType, Class<V> valueType, Map<String, ?> configs) {
		this(keyDeserializerClass, valueDeserializerClass, keyType, valueType);
		this.configs = configs;
	}

	// ------- Methods for deserialization -------

	@Override
	public TypeInformation<Tuple2<K,V>> getProducedType() {
		return this.producedType;
	}

	@Override
	public Tuple2<K, V> deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
		if(!isInitialized) {
			keyDeserializer = InstantiationUtil.instantiate(keyDeserializerClass, Deserializer.class);
			keyDeserializer.configure(configs, true);
			valueDeserializer = InstantiationUtil.instantiate(valueDeserializerClass, Deserializer.class);
			valueDeserializer.configure(configs, false);
			isInitialized = true;
		}
		K key = keyDeserializer.deserialize(topic, messageKey);
		V value = valueDeserializer.deserialize(topic, message);
		return Tuple2.of(key, value);
	}


	@Override
	public boolean isEndOfStream(Tuple2<K, V> nextElement) {
		return false;
	}
}
