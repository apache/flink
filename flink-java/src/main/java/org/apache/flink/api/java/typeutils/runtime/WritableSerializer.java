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

package org.apache.flink.api.java.typeutils.runtime;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import com.esotericsoftware.kryo.Kryo;

public class WritableSerializer<T extends Writable> extends TypeSerializer<T> {
	
	private static final long serialVersionUID = 1L;
	
	private final Class<T> typeClass;
	
	private transient Kryo kryo;
	
	private transient T copyInstance;
	
	public WritableSerializer(Class<T> typeClass) {
		this.typeClass = typeClass;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public T createInstance() {
		if(typeClass == NullWritable.class) {
			return (T) NullWritable.get();
		}
		return InstantiationUtil.instantiate(typeClass);
	}
	
	@Override
	public T copy(T from) {
		checkKryoInitialized();
		return this.kryo.copy(from);
	}
	
	@Override
	public T copy(T from, T reuse) {
		checkKryoInitialized();
		return this.kryo.copy(from);
	}
	
	@Override
	public int getLength() {
		return -1;
	}
	
	@Override
	public void serialize(T record, DataOutputView target) throws IOException {
		record.write(target);
	}
	
	@Override
	public T deserialize(DataInputView source) throws IOException {
		return deserialize(createInstance(), source);
	}
	
	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		reuse.readFields(source);
		return reuse;
	}
	
	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		ensureInstanceInstantiated();
		copyInstance.readFields(source);
		copyInstance.write(target);
	}
	
	@Override
	public boolean isImmutableType() {
		return false;
	}
	
	@Override
	public WritableSerializer<T> duplicate() {
		return new WritableSerializer<T>(typeClass);
	}
	
	// --------------------------------------------------------------------------------------------
	
	private void ensureInstanceInstantiated() {
		if (copyInstance == null) {
			copyInstance = createInstance();
		}
	}
	
	private void checkKryoInitialized() {
		if (this.kryo == null) {
			this.kryo = new Kryo();
			this.kryo.setAsmEnabled(true);
			this.kryo.register(typeClass);
		}
	}
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		return this.typeClass.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof WritableSerializer) {
			WritableSerializer<?> other = (WritableSerializer<?>) obj;

			return other.canEqual(this) && typeClass == other.typeClass;
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof WritableSerializer;
	}
}
