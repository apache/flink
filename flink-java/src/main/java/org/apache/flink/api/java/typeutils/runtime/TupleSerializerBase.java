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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Arrays;


public abstract class TupleSerializerBase<T> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;

	protected final Class<T> tupleClass;

	protected final TypeSerializer<Object>[] fieldSerializers;

	protected final int arity;

	protected final boolean stateful;


	@SuppressWarnings("unchecked")
	public TupleSerializerBase(Class<T> tupleClass, TypeSerializer<?>[] fieldSerializers) {
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
	public int getLength() {
		return -1;
	}

	public int getArity() {
		return arity;
	}

	// We use this in the Aggregate and Distinct Operators to create instances
	// of immutable Typles (i.e. Scala Tuples)
	public abstract T createInstance(Object[] fields);

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
		if (obj != null && obj instanceof TupleSerializerBase) {
			TupleSerializerBase<?> otherTS = (TupleSerializerBase<?>) obj;
			return (otherTS.tupleClass == this.tupleClass) && 
					Arrays.deepEquals(this.fieldSerializers, otherTS.fieldSerializers);
		}
		else {
			return false;
		}
	}
}
