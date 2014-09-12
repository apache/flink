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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.NullFieldException;


public final class TupleSerializer<T extends Tuple> extends TupleSerializerBase<T> {

	private static final long serialVersionUID = 1L;
	
	@SuppressWarnings("unchecked")
	public TupleSerializer(Class<T> tupleClass, TypeSerializer<?>[] fieldSerializers) {
		super(tupleClass, fieldSerializers);
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
	public void serialize(T value, DataOutputView target) throws IOException {
		for (int i = 0; i < arity; i++) {
			Object o = value.getField(i);
			try {
				fieldSerializers[i].serialize(o, target);
			} catch (NullPointerException npex) {
				throw new NullFieldException(i);
			}
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
}
