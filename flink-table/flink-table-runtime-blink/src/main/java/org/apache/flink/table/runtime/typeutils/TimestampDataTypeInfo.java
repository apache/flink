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
import org.apache.flink.table.data.TimestampData;

import java.util.Objects;

/**
 * TypeInformation for {@link TimestampData}.
 */
@Internal
public class TimestampDataTypeInfo extends TypeInformation<TimestampData> {

	private static final long serialVersionUID = 1L;

	private final int precision;

	public TimestampDataTypeInfo(int precision) {
		this.precision = precision;
	}

	@Override
	public boolean isBasicType() {
		return true;
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
	public Class<TimestampData> getTypeClass() {
		return TimestampData.class;
	}

	@Override
	public boolean isKeyType() {
		return true;
	}

	@Override
	public TypeSerializer<TimestampData> createSerializer(ExecutionConfig config) {
		return new TimestampDataSerializer(precision);
	}

	@Override
	public String toString() {
		return String.format("Timestamp(%d)", precision);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof TimestampDataTypeInfo)) {
			return false;
		}

		TimestampDataTypeInfo that = (TimestampDataTypeInfo) obj;
		return this.precision == that.precision;
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.getClass().getCanonicalName(), precision);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof TimestampDataTypeInfo;
	}

	public int getPrecision() {
		return precision;
	}
}
