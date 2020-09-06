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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public abstract class TupleSerializerBase<T> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;

	protected final Class<T> tupleClass;

	protected TypeSerializer<Object>[] fieldSerializers;

	protected final int arity;

	private int length = -2;

	@SuppressWarnings("unchecked")
	public TupleSerializerBase(Class<T> tupleClass, TypeSerializer<?>[] fieldSerializers) {
		this.tupleClass = checkNotNull(tupleClass);
		this.fieldSerializers = (TypeSerializer<Object>[]) checkNotNull(fieldSerializers);
		this.arity = fieldSerializers.length;
	}
	
	public Class<T> getTupleClass() {
		return this.tupleClass;
	}
	
	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public int getLength() {
		if (length == -2) {
			int sum = 0;
			for (TypeSerializer<Object> serializer : fieldSerializers) {
				if (serializer.getLength() > 0) {
					sum += serializer.getLength();
				} else {
					length = -1;
					return length;
				}
			}
			length = sum;
		}
		return length;
	}

	public int getArity() {
		return arity;
	}

	// We use this in the Aggregate and Distinct Operators to create instances
	// of immutable Tuples (i.e. Scala Tuples)
	public abstract T createInstance(Object[] fields);

	public abstract T createOrReuseInstance(Object[] fields, T reuse);

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		for (int i = 0; i < arity; i++) {
			fieldSerializers[i].copy(source, target);
		}
	}
	
	@Override
	public int hashCode() {
		return 31 * Arrays.hashCode(fieldSerializers) + Objects.hash(tupleClass, arity);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TupleSerializerBase) {
			TupleSerializerBase<?> other = (TupleSerializerBase<?>) obj;

			return tupleClass == other.tupleClass &&
				Arrays.equals(fieldSerializers, other.fieldSerializers) &&
				arity == other.arity;
		} else {
			return false;
		}
	}

	@VisibleForTesting
	public TypeSerializer<Object>[] getFieldSerializers() {
		return fieldSerializers;
	}
}
