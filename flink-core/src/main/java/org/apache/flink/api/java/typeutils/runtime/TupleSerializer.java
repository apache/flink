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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.NullFieldException;

@Internal
public class TupleSerializer<T extends Tuple> extends TupleSerializerBase<T> {

	private static final long serialVersionUID = 1L;
	
	public TupleSerializer(Class<T> tupleClass, TypeSerializer<?>[] fieldSerializers) {
		super(tupleClass, fieldSerializers);
	}

	@Override
	public TupleSerializer<T> duplicate() {
		boolean stateful = false;
		TypeSerializer<?>[] duplicateFieldSerializers = new TypeSerializer<?>[fieldSerializers.length];

		for (int i = 0; i < fieldSerializers.length; i++) {
			duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
			if (duplicateFieldSerializers[i] != fieldSerializers[i]) {
				// at least one of them is stateful
				stateful = true;
			}
		}

		if (stateful) {
			return new TupleSerializer<T>(tupleClass, duplicateFieldSerializers);
		} else {
			return this;
		}
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
	public T createInstance(Object[] fields) {

		try {
			T t = tupleClass.newInstance();

			for (int i = 0; i < arity; i++) {
				t.setField(fields[i], i);
			}

			return t;
		}
		catch (Exception e) {
			throw new RuntimeException("Cannot instantiate tuple.", e);
		}
	}

	@Override
	public T createOrReuseInstance(Object[] fields, T reuse) {
		for (int i = 0; i < arity; i++) {
			reuse.setField(fields[i], i);
		}
		return reuse;
	}

	@Override
	public T copy(T from) {
		T target = instantiateRaw();
		for (int i = 0; i < arity; i++) {
			Object copy = fieldSerializers[i].copy(from.getField(i));
			target.setField(copy, i);
		}
		return target;
	}
	
	@Override
	public T copy(T from, T reuse) {
		for (int i = 0; i < arity; i++) {
			Object copy = fieldSerializers[i].copy((Object)from.getField(i), reuse.getField(i));
			reuse.setField(copy, i);
		}
		
		return reuse;
	}

	@Override
	public void serialize(T value, DataOutputView target) throws IOException {
		for (int i = 0; i < arity; i++) {
			Object o = value.getField(i);
			try {
				fieldSerializers[i].serialize(o, target);
			} catch (NullPointerException npex) {
				throw new NullFieldException(i, npex);
			}
		}
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		T tuple = instantiateRaw();
		for (int i = 0; i < arity; i++) {
			Object field = fieldSerializers[i].deserialize(source);
			tuple.setField(field, i);
		}
		return tuple;
	}
	
	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		for (int i = 0; i < arity; i++) {
			Object field = fieldSerializers[i].deserialize(reuse.getField(i), source);
			reuse.setField(field, i);
		}
		return reuse;
	}
	
	private T instantiateRaw() {
		try {
			return tupleClass.newInstance();
		}
		catch (Exception e) {
			throw new RuntimeException("Cannot instantiate tuple.", e);
		}
	}

	@Override
	protected TupleSerializerBase<T> createSerializerInstance(Class<T> tupleClass, TypeSerializer<?>[] fieldSerializers) {
		return new TupleSerializer<>(tupleClass, fieldSerializers);
	}
}
