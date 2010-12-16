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

import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

/**
 * @author Erik Nijkamp
 * @author Alexander Alexandrov
 * @param <K>
 * @param <V>
 */
public class KeyValuePairDeserializer<K extends Key, V extends Value> implements RecordDeserializer<KeyValuePair<K, V>> {
	private ClassLoader classLoader;

	private Class<K> keyClass;

	private Class<V> valueClass;

	private final Class<KeyValuePair<K, V>> recordType = getKeyValuePairClass();

	public KeyValuePairDeserializer() {
	}

	public KeyValuePairDeserializer(Class<K> keyClass, Class<V> valueClass) {
		this.keyClass = keyClass;
		this.valueClass = valueClass;
	}

	
	private Class<KeyValuePair<K, V>> getKeyValuePairClass()
	{
		@SuppressWarnings("unchecked")
		Class<KeyValuePair<K, V>> clazz = (Class<KeyValuePair<K, V>>) (Class<?>) KeyValuePair.class;
		return clazz;
	}

	@Override
	public KeyValuePair<K, V> deserialize(DataInput in) {
		try {
			KeyValuePair<K, V> pair = getInstance();
			pair.read(in);
			return pair;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Class<KeyValuePair<K, V>> getRecordType() {
		return recordType;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void read(DataInput in) throws IOException {
		try {
			this.keyClass = (Class<K>) Class.forName(StringRecord.readString(in), true, this.classLoader);
			this.valueClass = (Class<V>) Class.forName(StringRecord.readString(in), true, this.classLoader);
		} catch (ClassNotFoundException e) {
			throw new IOException(StringUtils.stringifyException(e));
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		StringRecord.writeString(out, this.keyClass.getName());
		StringRecord.writeString(out, this.valueClass.getName());
	}

	@Override
	public void setClassLoader(ClassLoader classLoader) {

		this.classLoader = classLoader;
	}

	@Override
	public KeyValuePair<K, V> getInstance() {
		try {
			K key = keyClass.newInstance();
			V value = valueClass.newInstance();
			KeyValuePair<K, V> pair = new KeyValuePair<K, V>(key, value);
			return pair;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
