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

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.SingleThreadAccessCheckingTypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Objects;

/** Custom {@link TypeInformation} to test custom {@link TypeSerializer}. */
public class SingleThreadAccessCheckingTypeInfo<T> extends TypeInformation<T> {
	private final TypeInformation<T> originalTypeInformation;

	SingleThreadAccessCheckingTypeInfo(Class<T> clazz) {
		this(TypeInformation.of(clazz));
	}

	private SingleThreadAccessCheckingTypeInfo(TypeInformation<T> originalTypeInformation) {
		this.originalTypeInformation = originalTypeInformation;
	}

	@Override
	public boolean isBasicType() {
		return originalTypeInformation.isBasicType();
	}

	@Override
	public boolean isTupleType() {
		return originalTypeInformation.isTupleType();
	}

	@Override
	public int getArity() {
		return originalTypeInformation.getArity();
	}

	@Override
	public int getTotalFields() {
		return originalTypeInformation.getTotalFields();
	}

	@Override
	public Class<T> getTypeClass() {
		return originalTypeInformation.getTypeClass();
	}

	@Override
	public boolean isKeyType() {
		return originalTypeInformation.isKeyType();
	}

	@Override
	public TypeSerializer<T> createSerializer(ExecutionConfig config) {
		return new SingleThreadAccessCheckingTypeSerializer<>(originalTypeInformation.createSerializer(config));
	}

	@Override
	public String toString() {
		return originalTypeInformation.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o){
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SingleThreadAccessCheckingTypeInfo that = (SingleThreadAccessCheckingTypeInfo) o;
		return Objects.equals(originalTypeInformation, that.originalTypeInformation);
	}

	@Override
	public int hashCode() {
		return Objects.hash(originalTypeInformation);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof SingleThreadAccessCheckingTypeInfo;
	}
}
