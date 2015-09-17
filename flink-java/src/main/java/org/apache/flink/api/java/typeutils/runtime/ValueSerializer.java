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

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;
import org.apache.flink.util.InstantiationUtil;

import com.esotericsoftware.kryo.Kryo;

/**
 * Serializer for {@link Value} types. Uses the value's serialization methods, and uses
 * Kryo for deep object copies.
 *
 * @param <T> The type serialized.
 */
public class ValueSerializer<T extends Value> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;
	
	private final Class<T> type;
	
	private transient Kryo kryo;
	
	private transient T copyInstance;
	
	// --------------------------------------------------------------------------------------------
	
	public ValueSerializer(Class<T> type) {
		this.type = Preconditions.checkNotNull(type);
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public ValueSerializer<T> duplicate() {
		return new ValueSerializer<T>(type);
	}
	
	@Override
	public T createInstance() {
		return InstantiationUtil.instantiate(this.type);
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
	public void serialize(T value, DataOutputView target) throws IOException {
		value.write(target);
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		return deserialize(createInstance(), source);
	}
	
	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		reuse.read(source);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		if (this.copyInstance == null) {
			this.copyInstance = InstantiationUtil.instantiate(type);
		}
		
		this.copyInstance.read(source);
		this.copyInstance.write(target);
	}
	
	private void checkKryoInitialized() {
		if (this.kryo == null) {
			this.kryo = new Kryo();
			this.kryo.setAsmEnabled(true);
			this.kryo.register(type);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		return this.type.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ValueSerializer) {
			ValueSerializer<?> other = (ValueSerializer<?>) obj;

			return other.canEqual(this) && type == other.type;
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof ValueSerializer;
	}
}
