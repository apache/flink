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

package org.apache.flink.table.type;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Generic type.
 */
public class GenericType<T> implements AtomicType {

	private static final long serialVersionUID = 1L;

	private final TypeInformation<T> typeInfo;

	private transient TypeSerializer<T> serializer;

	public GenericType(Class<T> typeClass) {
		this(new GenericTypeInfo<>(typeClass));
	}

	public GenericType(TypeInformation<T> typeInfo) {
		this.typeInfo = checkNotNull(typeInfo);
	}

	public TypeInformation<T> getTypeInfo() {
		return typeInfo;
	}

	public Class<T> getTypeClass() {
		return typeInfo.getTypeClass();
	}

	public TypeSerializer<T> getSerializer() {
		if (serializer == null) {
			this.serializer = typeInfo.createSerializer(new ExecutionConfig());
		}
		return serializer;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		GenericType<?> that = (GenericType<?>) o;
		return typeInfo.equals(that.typeInfo);
	}

	@Override
	public int hashCode() {
		return typeInfo.hashCode();
	}

	@Override
	public String toString() {
		return "GenericType{" +
				"typeClass=" + typeInfo.getTypeClass() +
				'}';
	}
}
