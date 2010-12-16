/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.services.iomanager.Deserializer;
import eu.stratosphere.nephele.services.iomanager.RawComparator;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.iomanager.Serializer;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

/**
 * @author Alexander Alexandrov
 * @author Erik Nijkamp
 * @param <K>
 * @param <V>
 */
public class KeyValuePairSerializationFactory<K extends Key, V extends Value> implements
		SerializationFactory<KeyValuePair<K, V>> {
	public final SerializationFactory<K> keySerialization;

	public final SerializationFactory<V> valueSerialization;

	private final KeyValuePair<K, V> instance;

	public KeyValuePairSerializationFactory(SerializationFactory<K> keySerialization,
			SerializationFactory<V> valueSerialization) {
		this.keySerialization = keySerialization;
		this.valueSerialization = valueSerialization;
		this.instance = newInstance();
	}

	@Override
	public Deserializer<KeyValuePair<K, V>> getDeserializer() {
		return new Deserializer<KeyValuePair<K, V>>() {
			private final Deserializer<K> keyDeserializer = keySerialization.getDeserializer();

			private final Deserializer<V> valueDeserializer = valueSerialization.getDeserializer();

			@Override
			public void open(DataInput input) throws IOException {
				keyDeserializer.open(input);
				valueDeserializer.open(input);
			}

			@Override
			public void close() throws IOException {
				keyDeserializer.close();
				valueDeserializer.close();
			}

			@Override
			public KeyValuePair<K, V> deserialize(KeyValuePair<K, V> readable) throws IOException {
				// mutable deserialization
				if (readable == null) {
					readable = instance;
				}

				K key = readable.getKey();
				V value = readable.getValue();

				key = keyDeserializer.deserialize(key);
				value = valueDeserializer.deserialize(value);

				return readable;
			}
		};
	}

	@Override
	public Serializer<KeyValuePair<K, V>> getSerializer() {
		return new Serializer<KeyValuePair<K, V>>() {
			private final Serializer<K> keySerializer = keySerialization.getSerializer();

			private final Serializer<V> valueSerializer = valueSerialization.getSerializer();

			@Override
			public void open(DataOutput output) throws IOException {
				keySerializer.open(output);
				valueSerializer.open(output);
			}

			@Override
			public void close() throws IOException {
				keySerializer.close();
				valueSerializer.close();
			}

			@Override
			public void serialize(KeyValuePair<K, V> t) throws IOException {
				keySerializer.serialize(t.getKey());
				valueSerializer.serialize(t.getValue());
			}
		};
	}

	@Override
	public KeyValuePair<K, V> newInstance() {
		K key = keySerialization.newInstance();
		V value = valueSerialization.newInstance();

		return new KeyValuePair<K, V>(key, value);
	}

	@Override
	public RawComparator getRawComparator() {
		return null;
	}
}
