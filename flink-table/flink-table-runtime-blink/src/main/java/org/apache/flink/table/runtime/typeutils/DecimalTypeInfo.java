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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.dataformat.Decimal;

import java.util.Arrays;

/**
 * TypeInfo for Decimal.
 */
public class DecimalTypeInfo extends TypeInformation<Decimal> {

	public static DecimalTypeInfo of(int precision, int scale) {
		return new DecimalTypeInfo(precision, scale);
	}

	private final int precision;

	private final int scale;

	public DecimalTypeInfo(int precision, int scale) {
		this.precision = precision;
		this.scale = scale;
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
	public Class<Decimal> getTypeClass() {
		return Decimal.class;
	}

	@Override
	public boolean isKeyType() {
		return true;
	}

	@Override
	public TypeSerializer<Decimal> createSerializer(ExecutionConfig config) {
		return new DecimalSerializer(precision, scale);
	}

	@Override
	public String toString() {
		return String.format("Decimal(%d,%d)", precision, scale);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof DecimalTypeInfo)) {
			return false;
		}
		DecimalTypeInfo that = (DecimalTypeInfo) obj;
		return this.precision == that.precision && this.scale == that.scale;
	}

	@Override
	public int hashCode() {
		int h0 = this.getClass().getCanonicalName().hashCode();
		return Arrays.hashCode(new int[]{h0, precision, scale});
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof DecimalTypeInfo;
	}

	public int precision() {
		return precision;
	}

	public int scale() {
		return scale;
	}
}
