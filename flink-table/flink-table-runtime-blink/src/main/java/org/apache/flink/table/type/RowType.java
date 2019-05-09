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
import org.apache.flink.table.typeutils.BaseRowSerializer;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * Row type for row.
 *
 * <p>It's internal data structure is BaseRow, and it's external data structure is {@link Row}.
 */
public class RowType implements InternalType {

	private static final long serialVersionUID = 1L;

	private final InternalType[] types;

	private final String[] fieldNames;

	private transient BaseRowSerializer baseRowSerializer;

	public RowType(InternalType... types) {
		this(types, generateDefaultFieldNames(types.length));
	}

	public RowType(InternalType[] types, String[] fieldNames) {
		this.types = types;
		this.fieldNames = fieldNames;
		if (types.length != fieldNames.length) {
			throw new IllegalArgumentException("Types should be the same length as names, types is: "
					+ Arrays.toString(types) + ", and the names: " + Arrays.toString(fieldNames));
		}
	}

	public static String[] generateDefaultFieldNames(int length) {
		String[] fieldNames = new String[length];
		for (int i = 0; i < length; i++) {
			fieldNames[i] = "f" + i;
		}
		return fieldNames;
	}

	public BaseRowSerializer getBaseRowSerializer() {
		if (baseRowSerializer == null) {
			this.baseRowSerializer =
					(BaseRowSerializer) TypeConverters.createInternalTypeInfoFromInternalType(this)
							.createSerializer(new ExecutionConfig());
		}
		return baseRowSerializer;
	}

	public int getArity() {
		return types.length;
	}

	public InternalType[] getFieldTypes() {
		return types;
	}

	public InternalType getTypeAt(int i) {
		return types[i];
	}

	public String[] getFieldNames() {
		return fieldNames;
	}

	public int getFieldIndex(String fieldName) {
		for (int i = 0; i < fieldNames.length; i++) {
			if (fieldNames[i].equals(fieldName)) {
				return i;
			}
		}
		return -1;
	}

	public BaseRowTypeInfo toTypeInfo() {
		return new BaseRowTypeInfo(types, fieldNames);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		RowType that = (RowType) o;

		// RowType comparisons should not compare names and are compatible with the behavior of CompositeTypeInfo.
		return Arrays.equals(getFieldTypes(), that.getFieldTypes()) &&
			Arrays.equals(getFieldNames(), that.getFieldNames());
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(types);
	}

	@Override
	public String toString() {
		return "RowType{" +
				"types=" + Arrays.toString(types) +
				", fieldNames=" + Arrays.toString(fieldNames) +
				'}';
	}
}
