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
 * @author Erik Nijkamp
 * @param <T>
 */
public class DefaultRecordDeserializer<T extends IOReadableWritable> implements RecordDeserializer<T> {
	private Class<T> recordClass;

	private ClassLoader classLoader;

	public DefaultRecordDeserializer() {

	}

	public DefaultRecordDeserializer(Class<T> recordClass) {
		this.recordClass = recordClass;
	}

	@Override
	public T deserialize(DataInput in) throws IOException {
		T record = getInstance();
		record.read(in);
		return record;
	}

	@Override
	public Class<T> getRecordType() {
		return recordClass;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void read(DataInput in) throws IOException {
		final String typeClassName = StringRecord.readString(in);
		try {
			this.recordClass = (Class<T>) Class.forName(typeClassName, true, this.classLoader);
		} catch (ClassNotFoundException e) {
			throw new IOException(StringUtils.stringifyException(e));
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		StringRecord.writeString(out, this.recordClass.getName());
	}

	@Override
	public void setClassLoader(ClassLoader classLoader) {

		this.classLoader = classLoader;
	}

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