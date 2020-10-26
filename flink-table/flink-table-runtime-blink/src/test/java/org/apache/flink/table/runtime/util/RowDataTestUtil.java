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

package org.apache.flink.table.runtime.util;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

/**
 * Utility for RowData.
 */
public class RowDataTestUtil {

	public static String rowToString(RowData value, InternalTypeInfo<RowData> rowTypeInfo, TimeZone tz) {
		return rowToString(value, rowTypeInfo, tz, true);
	}

	public static String rowToString(RowData value, InternalTypeInfo<RowData> rowTypeInfo, TimeZone tz, boolean withHeader) {
		GenericRowData genericRow = toGenericRowDeeply(value, rowTypeInfo.toRowFieldTypes());
		return genericRowToString(genericRow, tz, withHeader);
	}

	private static String fieldToString(Object field, TimeZone tz) {
		if (field instanceof Date || field instanceof Time || field instanceof Timestamp) {
			// TODO support after FLINK-11898 is merged
			throw new UnsupportedOperationException();
		} else {
			return StringUtils.arrayAwareToString(field);
		}
	}

	private static String genericRowToString(GenericRowData row, TimeZone tz, boolean withHeader) {
		StringBuilder sb = new StringBuilder();
		if (withHeader) {
			sb.append(row.getRowKind().shortString());
		}
		sb.append("(");
		for (int i = 0; i < row.getArity(); i++) {
			if (i > 0) {
				sb.append(",");
			}
			sb.append(fieldToString(row.getField(i), tz));
		}
		sb.append(")");
		return sb.toString();
	}

	public static GenericRowData toGenericRowDeeply(RowData rowData, LogicalType[] types) {
		return toGenericRowDeeply(rowData, Arrays.asList(types));
	}

	public static GenericRowData toGenericRowDeeply(RowData rowData, List<LogicalType> types) {
		if (rowData instanceof GenericRowData) {
			return (GenericRowData) rowData;
		} else {
			int fieldNum = rowData.getArity();
			GenericRowData row = new GenericRowData(fieldNum);
			row.setRowKind(rowData.getRowKind());
			for (int i = 0; i < fieldNum; i++) {
				if (rowData.isNullAt(i)) {
					row.setField(i, null);
				} else {
					LogicalType type = types.get(i);
					RowData.FieldGetter fieldGetter = RowData.createFieldGetter(type, i);
					Object o = fieldGetter.getFieldOrNull(rowData);
					if (type instanceof RowType) {
						o = toGenericRowDeeply((RowData) o, type.getChildren());
					}
					row.setField(i, o);
				}
			}
			return row;
		}
	}

	public static void write(BinaryWriter writer, int pos, Object o, LogicalType type) {
		BinaryWriter.write(writer, pos, o, type, InternalSerializers.create(type));
	}

}
