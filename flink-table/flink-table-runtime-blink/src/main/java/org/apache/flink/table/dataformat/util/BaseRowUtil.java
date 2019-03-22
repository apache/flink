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

package org.apache.flink.table.dataformat.util;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.typeutils.DecimalTypeInfo;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.table.util.TypeUtils;

import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

/**
 * Util for base row.
 */
public final class BaseRowUtil {

	public static GenericRow toGenericRow(
		BaseRow baseRow,
		TypeInformation[] typeInfos,
		TypeSerializer[] typeSerializers) {
		if (baseRow instanceof GenericRow) {
			return (GenericRow) baseRow;
		} else {
			GenericRow row = new GenericRow(baseRow.getArity());
			row.setHeader(baseRow.getHeader());
			for (int i = 0; i < row.getArity(); i++) {
				if (baseRow.isNullAt(i)) {
					row.update(i, null);
				} else {
					row.update(i, get(baseRow, i, typeInfos[i], typeSerializers[i]));
				}
			}
			return row;
		}
	}

	public static Object get(BaseRow row, int ordinal, TypeInformation type, TypeSerializer serializer) {
		if (row instanceof GenericRow) {
			return ((GenericRow) row).getField(ordinal);
		}
		if (type.equals(Types.BOOLEAN)) {
			return row.getBoolean(ordinal);
		} else if (type.equals(Types.BYTE)) {
			return row.getByte(ordinal);
		} else if (type.equals(Types.SHORT)) {
			return row.getShort(ordinal);
		} else if (type.equals(Types.INT)) {
			return row.getInt(ordinal);
		} else if (type.equals(Types.LONG)) {
			return row.getLong(ordinal);
		} else if (type.equals(Types.FLOAT)) {
			return row.getFloat(ordinal);
		} else if (type.equals(Types.DOUBLE)) {
			return row.getDouble(ordinal);
		} else if (type instanceof DecimalTypeInfo) {
			DecimalTypeInfo dt = (DecimalTypeInfo) type;
			return row.getDecimal(ordinal, dt.precision(), dt.scale());
		} else if (type.equals(Types.STRING)) {
			return row.getString(ordinal);
		} else if (type.equals(BasicTypeInfo.CHAR_TYPE_INFO)) {
			return row.getChar(ordinal);
		} else if (type.equals(TimeIndicatorTypeInfo.ROWTIME_INDICATOR)) {
			return row.getLong(ordinal);
		} else if (type.equals(SqlTimeTypeInfo.DATE)) {
			return row.getInt(ordinal);
		} else if (type.equals(SqlTimeTypeInfo.TIME)) {
			return row.getInt(ordinal);
		} else if (type.equals(SqlTimeTypeInfo.TIMESTAMP)) {
			return row.getLong(ordinal);
		} else if (type.equals(BYTE_PRIMITIVE_ARRAY_TYPE_INFO)) {
			return row.getBinary(ordinal);
		} else if (TypeUtils.isInternalArrayType(type)) {
			return row.getArray(ordinal);
		} else if (type instanceof MapTypeInfo) {
			return row.getMap(ordinal);
		} else if (TypeUtils.isInternalCompositeType(type)) {
			return row.getRow(ordinal, type.getArity());
		} else {
			return row.getGeneric(ordinal);
		}
	}

}
