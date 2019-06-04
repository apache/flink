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

package org.apache.flink.table.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.types.TypeInfoLogicalTypeConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.HashSet;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base row type info.
 */
public class BaseRowTypeInfo extends TupleTypeInfoBase<BaseRow> {

	private static final long serialVersionUID = 1L;

	private final String[] fieldNames;
	private final LogicalType[] logicalTypes;

	public BaseRowTypeInfo(LogicalType... logicalTypes) {
		this(logicalTypes, generateDefaultFieldNames(logicalTypes.length));
	}

	public BaseRowTypeInfo(LogicalType[] logicalTypes, String[] fieldNames) {
		super(BaseRow.class, Arrays.stream(logicalTypes)
				.map(TypeInfoLogicalTypeConverter::fromLogicalTypeToTypeInfo)
				.toArray(TypeInformation[]::new));
		this.logicalTypes = logicalTypes;
		checkNotNull(fieldNames, "FieldNames should not be null.");
		checkArgument(logicalTypes.length == fieldNames.length,
				"Number of field types and names is different.");
		checkArgument(!hasDuplicateFieldNames(fieldNames),
				"Field names are not unique.");
		this.fieldNames = Arrays.copyOf(fieldNames, fieldNames.length);
	}

	public static String[] generateDefaultFieldNames(int length) {
		String[] fieldNames = new String[length];
		for (int i = 0; i < length; i++) {
			fieldNames[i] = "f" + i;
		}
		return fieldNames;
	}

	@Override
	public <X> TypeInformation<X> getTypeAt(String fieldExpression) {
		throw new UnsupportedOperationException("Not support!");
	}

	@Override
	public TypeComparator<BaseRow> createComparator(
		int[] logicalKeyFields,
		boolean[] orders,
		int logicalFieldOffset,
		ExecutionConfig config) {
		// TODO support it.
		throw new UnsupportedOperationException("Not support yet!");
	}

	@Override
	public String[] getFieldNames() {
		return fieldNames;
	}

	@Override
	public int getFieldIndex(String fieldName) {
		for (int i = 0; i < fieldNames.length; i++) {
			if (fieldNames[i].equals(fieldName)) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof BaseRowTypeInfo;
	}

	@Override
	public int hashCode() {
		return 31 * super.hashCode() + Arrays.hashCode(fieldNames);
	}

	@Override
	public String toString() {
		StringBuilder bld = new StringBuilder("BaseRow");
		if (types.length > 0) {
			bld.append('(').append(fieldNames[0]).append(": ").append(types[0]);

			for (int i = 1; i < types.length; i++) {
				bld.append(", ").append(fieldNames[i]).append(": ").append(types[i]);
			}

			bld.append(')');
		}
		return bld.toString();
	}

	/**
	 * Returns the field types of the row. The order matches the order of the field names.
	 */
	public TypeInformation<?>[] getFieldTypes() {
		return types;
	}

	private boolean hasDuplicateFieldNames(String[] fieldNames) {
		HashSet<String> names = new HashSet<>();
		for (String field : fieldNames) {
			if (!names.add(field)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public CompositeType.TypeComparatorBuilder<BaseRow> createTypeComparatorBuilder() {
		throw new UnsupportedOperationException("Not support!");
	}

	@Override
	public BaseRowSerializer createSerializer(ExecutionConfig config) {
		return new BaseRowSerializer(config, logicalTypes);
	}

	public LogicalType[] getLogicalTypes() {
		return logicalTypes;
	}

	public RowType toRowType() {
		return RowType.of(logicalTypes, fieldNames);
	}

	public static BaseRowTypeInfo of(RowType rowType) {
		return new BaseRowTypeInfo(
				rowType.getChildren().toArray(new LogicalType[0]),
				rowType.getFieldNames().toArray(new String[0]));
	}
}
