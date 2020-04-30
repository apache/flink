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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/**
 * Type information that wraps an existing serializer.
 */
@Internal
public final class WrapperTypeInfo<T> extends TypeInformation<T> {

	private static final String FORMAT = "%s(%s, %s)";

	private final Class<T> typeClass;

	private final TypeSerializer<T> typeSerializer;

	public WrapperTypeInfo(Class<T> typeClass, TypeSerializer<T> typeSerializer) {
		this.typeClass = Preconditions.checkNotNull(typeClass);
		this.typeSerializer = Preconditions.checkNotNull(typeSerializer);
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
	public int getArity() {
		return 1;
	}

	@Override
	public int getTotalFields() {
		return 1;
	}

	@Override
	public Class<T> getTypeClass() {
		return typeClass;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<T> createSerializer(ExecutionConfig config) {
		return typeSerializer;
	}

	@Override
	public String toString() {
		return String.format(
			FORMAT,
			this.getClass().getSimpleName(),
			typeClass.getName(),
			typeSerializer.getClass().getName());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final WrapperTypeInfo<?> that = (WrapperTypeInfo<?>) o;
		return typeClass.equals(that.typeClass) && typeSerializer.equals(that.typeSerializer);
	}

	@Override
	public int hashCode() {
		return Objects.hash(typeClass, typeSerializer);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof WrapperTypeInfo;
	}
}
