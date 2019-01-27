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

package org.apache.flink.table.typeutils.ordered;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Type information for Java SQL Date/Time/Timestamp.
 */
@Internal
public class OrderedSqlTimeTypeInfo<T> extends TypeInformation<T> {

	private static final long serialVersionUID = -132955295409131880L;

	public static final OrderedSqlTimeTypeInfo<Date>
		ASC_DATE = new OrderedSqlTimeTypeInfo<>(Date.class, OrderedSqlDateSerializer.ASC_INSTANCE);
	public static final OrderedSqlTimeTypeInfo<Date>
		DESC_DATE = new OrderedSqlTimeTypeInfo<>(Date.class, OrderedSqlDateSerializer.DESC_INSTANCE);

	public static final OrderedSqlTimeTypeInfo<Time>
		ASC_TIME = new OrderedSqlTimeTypeInfo<>(Time.class, OrderedSqlTimeSerializer.ASC_INSTANCE);
	public static final OrderedSqlTimeTypeInfo<Time>
		DESC_TIME = new OrderedSqlTimeTypeInfo<>(Time.class, OrderedSqlTimeSerializer.DESC_INSTANCE);

	public static final OrderedSqlTimeTypeInfo<Timestamp>
		ASC_TIMESTAMP = new OrderedSqlTimeTypeInfo<>(Timestamp.class, OrderedSqlTimestampSerializer.ASC_INSTANCE);
	public static final OrderedSqlTimeTypeInfo<Timestamp>
		DESC_TIMESTAMP = new OrderedSqlTimeTypeInfo<>(Timestamp.class, OrderedSqlTimestampSerializer.DESC_INSTANCE);

	// --------------------------------------------------------------------------------------------

	private final Class<T> clazz;

	private final TypeSerializer<T> serializer;

	protected OrderedSqlTimeTypeInfo(Class<T> clazz, TypeSerializer<T> serializer) {
		this.clazz = checkNotNull(clazz);
		this.serializer = checkNotNull(serializer);
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
		return clazz;
	}

	@Override
	public boolean isKeyType() {
		return true;
	}

	@Override
	public TypeSerializer<T> createSerializer(ExecutionConfig executionConfig) {
		return serializer;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return Objects.hash(clazz, serializer);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof OrderedSqlTimeTypeInfo;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof OrderedSqlTimeTypeInfo) {
			@SuppressWarnings("unchecked")
			OrderedSqlTimeTypeInfo<T>
				other = (OrderedSqlTimeTypeInfo<T>) obj;

			return other.canEqual(this) &&
				this.clazz == other.clazz &&
				serializer.equals(other.serializer);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return clazz.getSimpleName();
	}
}
