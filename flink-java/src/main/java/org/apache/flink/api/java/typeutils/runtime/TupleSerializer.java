/**
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
import java.util.Arrays;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;


public final class TupleSerializer<T extends Tuple> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;
	
	
	private final Class<T> tupleClass;
	
	private final TypeSerializer<Object>[] fieldSerializers;
	
	private final int arity;
	
	private final boolean stateful;
	
	
	@SuppressWarnings("unchecked")
	public TupleSerializer(Class<T> tupleClass, TypeSerializer<?>[] fieldSerializers) {
		this.tupleClass = tupleClass;
		this.fieldSerializers = (TypeSerializer<Object>[]) fieldSerializers;
		this.arity = fieldSerializers.length;
		
		boolean stateful = false;
		for (TypeSerializer<?> ser : fieldSerializers) {
			if (ser.isStateful()) {
				stateful = true;
				break;
			}
		}
		this.stateful = stateful;
	}
	
	
	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public boolean isStateful() {
		return this.stateful;
	}
	
	
	@Override
	public T createInstance() {
		try {
			T t = tupleClass.newInstance();
		
			for (int i = 0; i < arity; i++) {
				t.setField(fieldSerializers[i].createInstance(), i);
			}
			
			return t;
		}
		catch (Exception e) {
			throw new RuntimeException("Cannot instantiate tuple.", e);
		}
	}

	@Override
	public T copy(T from, T reuse) {
		for (int i = 0; i < arity; i++) {
			Object copy = fieldSerializers[i].copy(from.getField(i), reuse.getField(i));
			reuse.setField(copy, i);
		}
		
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}


	@Override
	public void serialize(T value, DataOutputView target) throws IOException {
		for (int i = 0; i < arity; i++) {
			Object o = value.getField(i);
			fieldSerializers[i].serialize(o, target);
		}
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		for (int i = 0; i < arity; i++) {
			Object field = fieldSerializers[i].deserialize(reuse.getField(i), source);
			reuse.setField(field, i);
		}
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		for (int i = 0; i < arity; i++) {
			fieldSerializers[i].copy(source, target);
		}
	}
	
	@Override
	public int hashCode() {
		int hashCode = arity * 47;
		for (TypeSerializer<?> ser : this.fieldSerializers) {
			hashCode = (hashCode << 7) | (hashCode >>> -7);
			hashCode += ser.hashCode();
		}
		return hashCode;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof TupleSerializer) {
			TupleSerializer<?> otherTS = (TupleSerializer<?>) obj;
			return (otherTS.tupleClass == this.tupleClass) && 
					Arrays.deepEquals(this.fieldSerializers, otherTS.fieldSerializers);
		}
		else {
			return false;
		}
	}
}
