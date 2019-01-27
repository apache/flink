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
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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

			return other.canEqual(this) &&
				tupleClass == other.tupleClass &&
				Arrays.equals(fieldSerializers, other.fieldSerializers) &&
				arity == other.arity;
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof TupleSerializerBase;
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting & compatibility
	// --------------------------------------------------------------------------------------------

	@Override
	public TupleSerializerConfigSnapshot<T> snapshotConfiguration() {
		return new TupleSerializerConfigSnapshot<>(tupleClass, fieldSerializers);
	}

	@SuppressWarnings("unchecked")
	@Override
	public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof TupleSerializerConfigSnapshot) {
			final TupleSerializerConfigSnapshot<T> config = (TupleSerializerConfigSnapshot<T>) configSnapshot;

			if (tupleClass.equals(config.getTupleClass())) {
				List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> previousFieldSerializersAndConfigs =
					((TupleSerializerConfigSnapshot) configSnapshot).getNestedSerializersAndConfigs();

				if (previousFieldSerializersAndConfigs.size() == fieldSerializers.length) {

					TypeSerializer<Object>[] convertFieldSerializers = new TypeSerializer[fieldSerializers.length];
					boolean requiresMigration = false;
					CompatibilityResult<Object> compatResult;
					int i = 0;
					for (Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> f : previousFieldSerializersAndConfigs) {
						compatResult = CompatibilityUtil.resolveCompatibilityResult(
								f.f0,
								UnloadableDummyTypeSerializer.class,
								f.f1,
								fieldSerializers[i]);

						if (compatResult.isRequiresMigration()) {
							requiresMigration = true;

							if (compatResult.getConvertDeserializer() != null) {
								convertFieldSerializers[i] =
									new TypeDeserializerAdapter<>(compatResult.getConvertDeserializer());
							} else {
								return CompatibilityResult.requiresMigration();
							}
						}

						i++;
					}

					if (!requiresMigration) {
						return CompatibilityResult.compatible();
					} else {
						return CompatibilityResult.requiresMigration(
							createSerializerInstance(tupleClass, convertFieldSerializers));
					}
				}
			}
		}

		return CompatibilityResult.requiresMigration();
	}

	protected abstract TupleSerializerBase<T> createSerializerInstance(Class<T> tupleClass, TypeSerializer<?>[] fieldSerializers);

	@VisibleForTesting
	public TypeSerializer<Object>[] getFieldSerializers() {
		return fieldSerializers;
	}
}
