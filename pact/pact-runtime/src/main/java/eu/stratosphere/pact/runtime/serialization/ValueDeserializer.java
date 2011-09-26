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
import eu.stratosphere.pact.common.type.Value;

/**
 * Deserializer for {@link Value} objects. 
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 * @param <V>
 */
public class ValueDeserializer<V extends Value> implements RecordDeserializer<V> {

	private ClassLoader classLoader;
	private Class<V> valueClass;

	public ValueDeserializer() {
	}

	public ValueDeserializer(Class<V> valueClass) {
		this.valueClass = valueClass;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.RecordDeserializer#deserialize(java.io.DataInput)
	 */
	@Override
	public V deserialize(V target, DataInput in) {
		try {
			V value = getInstance();
			value.read(in);
			return value;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void read(DataInput in) throws IOException {
		try {
			this.valueClass = (Class<V>) Class.forName(StringRecord.readString(in), true, this.classLoader);
		} catch (ClassNotFoundException e) {
			throw new IOException(StringUtils.stringifyException(e));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		StringRecord.writeString(out, this.valueClass.getName());
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.RecordDeserializer#setClassLoader(java.lang.ClassLoader)
	 */
	@Override
	public void setClassLoader(ClassLoader classLoader) {

		this.classLoader = classLoader;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.RecordDeserializer#getInstance()
	 */
	@Override
	public V getInstance() {
		try {
			return valueClass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
