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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.drivers.transform;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.LongValueComparator;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.LongValue;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;

/**
 * Fix for {@link LongValue#hashCode()}.
 */
@TypeInfo(LongValueWithProperHashCode.LongValueWithProperHashCodeTypeInfoFactory.class)
public class LongValueWithProperHashCode
extends LongValue {

	@Override
	public int hashCode() {
		return (int) (this.getValue() ^ this.getValue() >>> 32);
	}

	/**
	 * Initializes the encapsulated long with 0.
	 */
	public LongValueWithProperHashCode() {
		super();
	}

	/**
	 * Initializes the encapsulated long with the specified value.
	 *
	 * @param value Initial value of the encapsulated long.
	 */
	public LongValueWithProperHashCode(final long value) {
		super(value);
	}

	/**
	 * Used by {@link TypeExtractor} to create a {@link TypeInformation} for
	 * {@link LongValueWithProperHashCode}.
	 */
	public static class LongValueWithProperHashCodeTypeInfoFactory extends TypeInfoFactory<LongValueWithProperHashCode> {
		@Override
		public TypeInformation<LongValueWithProperHashCode> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
			@SuppressWarnings("unchecked")
			TypeInformation<LongValueWithProperHashCode> typeInfo = new LongValueWithProperHashCodeTypeInfo();

			return typeInfo;
		}
	}

	/**
	 * A {@link TypeInformation} for the {@link LongValueWithProperHashCode} type.
	 *
	 * @see org.apache.flink.api.java.typeutils.ValueTypeInfo#LONG_VALUE_TYPE_INFO
	 */
	public static class LongValueWithProperHashCodeTypeInfo extends TypeInformation<LongValueWithProperHashCode> implements AtomicType<LongValueWithProperHashCode> {
		private static final long serialVersionUID = 1L;

		@Override
		public int getArity() {
			return 1;
		}

		@Override
		public int getTotalFields() {
			return 1;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Class<LongValueWithProperHashCode> getTypeClass() {
			return LongValueWithProperHashCode.class;
		}

		@Override
		public boolean isBasicType() {
			return false;
		}

		@Override
		public boolean isTupleType() {
			return false;
		}

		@Override
		public boolean isKeyType() {
			return Comparable.class.isAssignableFrom(LongValueWithProperHashCode.class);
		}

		@Override
		@SuppressWarnings("unchecked")
		public TypeSerializer<LongValueWithProperHashCode> createSerializer(ExecutionConfig executionConfig) {
			return new LongValueWithProperHashCodeSerializer();
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public TypeComparator<LongValueWithProperHashCode> createComparator(boolean sortOrderAscending, ExecutionConfig executionConfig) {
			return (TypeComparator<LongValueWithProperHashCode>) (TypeComparator<?>) new LongValueComparator(sortOrderAscending);
		}

		@Override
		public Map<String, TypeInformation<?>> getGenericParameters() {
			return Collections.emptyMap();
		}

		// --------------------------------------------------------------------------------------------

		@Override
		public int hashCode() {
			return 0;
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof LongValueWithProperHashCodeTypeInfo;
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof LongValueWithProperHashCodeTypeInfo;
		}

		@Override
		public String toString() {
			return LongValueWithProperHashCodeTypeInfo.class.getSimpleName();
		}
	}

	/**
	 * Serializer for {@link LongValueWithProperHashCode}.
	 *
	 * @see org.apache.flink.api.common.typeutils.base.LongValueSerializer
	 */
	public static final class LongValueWithProperHashCodeSerializer extends TypeSerializerSingleton<LongValueWithProperHashCode> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public LongValueWithProperHashCode createInstance() {
			return new LongValueWithProperHashCode();
		}

		@Override
		public LongValueWithProperHashCode copy(LongValueWithProperHashCode from) {
			return copy(from, new LongValueWithProperHashCode());
		}

		@Override
		public LongValueWithProperHashCode copy(LongValueWithProperHashCode from, LongValueWithProperHashCode reuse) {
			reuse.setValue(from.getValue());
			return reuse;
		}

		@Override
		public int getLength() {
			return 8;
		}

		@Override
		public void serialize(LongValueWithProperHashCode record, DataOutputView target) throws IOException {
			record.write(target);
		}

		@Override
		public LongValueWithProperHashCode deserialize(DataInputView source) throws IOException {
			return deserialize(new LongValueWithProperHashCode(), source);
		}

		@Override
		public LongValueWithProperHashCode deserialize(LongValueWithProperHashCode reuse, DataInputView source) throws IOException {
			reuse.read(source);
			return reuse;
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			target.writeLong(source.readLong());
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof LongValueWithProperHashCodeSerializer;
		}

		@Override
		protected boolean isCompatibleSerializationFormatIdentifier(String identifier) {
			return super.isCompatibleSerializationFormatIdentifier(identifier)
				|| identifier.equals(LongSerializer.class.getCanonicalName());
		}
	}
}
