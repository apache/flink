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

package org.apache.flink.table.runtime.types;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * For CRow TypesInfo.
 */
@PublicEvolving
public class CRowTypeInfo extends CompositeType<CRow> {

	public RowTypeInfo rowType;

	public CRowTypeInfo(Class<CRow> typeClass) {
		super(typeClass);
	}

	public CRowTypeInfo(RowTypeInfo rowType) {
		super(CRow.class);
		this.rowType = rowType;
	}

	public static CRowTypeInfo of(TypeInformation<Row> rowType) throws Exception {
		if (rowType.getClass().equals(RowTypeInfo.class)) {
			return new CRowTypeInfo((RowTypeInfo) rowType);
		}
		throw new Exception("No other types other than RowTypeInfo are supported");
	}

	@Override
	public String[] getFieldNames() {
		return rowType.getFieldNames();
	}

	@Override
	public int getFieldIndex(String fieldName) {
		return rowType.getFieldIndex(fieldName);
	}

	@Override
	public <X> TypeInformation<X> getTypeAt(String fieldExpression) {
		return rowType.getTypeAt(fieldExpression);
	}

	@Override
	public <X> TypeInformation<X> getTypeAt(int pos) {
		return rowType.getTypeAt(pos);
	}

	@Override
	public void getFlatFields(String fieldExpression, int offset, List<FlatFieldDescriptor> result) {
		rowType.getFlatFields(fieldExpression, offset, result);
	}

	@Override
	public boolean isBasicType() {
		return rowType.isBasicType();
	}

	@Override
	public boolean isTupleType() {
		return rowType.isTupleType();
	}

	@Override
	public int getArity() {
		return rowType.getArity();
	}

	@Override
	public int getTotalFields() {
		return rowType.getTotalFields();
	}

	@Override
	public TypeSerializer<CRow> createSerializer(ExecutionConfig config) {
		return new CRowSerializer(rowType.createSerializer(config));
	}

	@Override
	protected TypeComparatorBuilder<CRow> createTypeComparatorBuilder() {
		return null;
	}

	@Override
	public TypeComparator<CRow> createComparator(int[] logicalKeyFields, boolean[] orders, int logicalFieldOffset, ExecutionConfig config) {
		TypeComparator<Row> rowComparator = rowType.createComparator(
			logicalKeyFields,
			orders,
			logicalFieldOffset,
			config);

		return new CRowComparator(rowComparator);
	}

	@Override
	public boolean equals(Object obj) {
		if (this.canEqual(obj)) {
			return rowType.equals(((CRowTypeInfo) obj).rowType);
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj.getClass().equals(CRowTypeInfo.class);
	}
}
