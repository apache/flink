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

package eu.stratosphere.nephele.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * The default implementation for {@link RecordDeserializer}. It is suitable for all non parameterized record types.
 * <p>
 * This class is in general not thread-safe.
 * 
 * @author Erik Nijkamp
 * @param <T>
 *        the type of record deserialized by this record deserializer
 */
public class DefaultRecordDeserializer<T extends IOReadableWritable> implements RecordDeserializer<T> {

	/**
	 * The type of record deserialized by this record deserializer.
	 */
	private Class<T> recordClass;

	/**
	 * The class loader that shall be used to look for <code>recordClass</code>.
	 */
	private ClassLoader classLoader;

	/**
	 * Empty constructor used for serialization/deserialization.
	 */
	public DefaultRecordDeserializer() {
	}

	/**
	 * Constructs a new default record deserializer.
	 * 
	 * @param recordClass
	 *        the type of record to be deserialized
	 */
	public DefaultRecordDeserializer(final Class<T> recordClass) {
		this.recordClass = recordClass;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T deserialize(T target, final DataInput in) throws IOException {

		if (target == null) {
			target = getInstance();
		}
		target.read(in);
		return target;
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void read(final DataInput in) throws IOException {
		final String typeClassName = StringRecord.readString(in);
		try {
			this.recordClass = (Class<T>) Class.forName(typeClassName, true, this.classLoader);
		} catch (ClassNotFoundException e) {
			throw new IOException(StringUtils.stringifyException(e));
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		StringRecord.writeString(out, this.recordClass.getName());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setClassLoader(final ClassLoader classLoader) {

		this.classLoader = classLoader;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T getInstance() {
		T record;
		try {
			record = recordClass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return record;
	}
}