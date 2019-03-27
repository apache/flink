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

package org.apache.flink.table.util;

import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.TypeGetterSetters;
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.StringUtils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;

/**
 * Utility for BaseRow.
 */
public class BaseRowTestUtil {

	public static String baseRowToString(BaseRow value, BaseRowTypeInfo rowTypeInfo, TimeZone tz) {
		return baseRowToString(value, rowTypeInfo, tz, true);
	}

	public static String baseRowToString(BaseRow value, BaseRowTypeInfo rowTypeInfo, TimeZone tz, boolean withHeader) {
		GenericRow genericRow = toGenericRow(value, rowTypeInfo);
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

	private static String genericRowToString(GenericRow row, TimeZone tz, boolean withHeader) {
		StringBuilder sb = new StringBuilder();
		if (withHeader) {
			sb.append(row.getHeader()).append("|");
		}
		for (int i = 0; i < row.getArity(); i++) {
			if (i > 0) {
				sb.append(",");
			}
			sb.append(fieldToString(row.getField(i), tz));
		}
		return sb.toString();
	}

	private static GenericRow toGenericRow(BaseRow baseRow, BaseRowTypeInfo baseRowTypeInfo) {
		if (baseRow instanceof GenericRow) {
			return (GenericRow) baseRow;
		} else {
			int fieldNum = baseRow.getArity();
			GenericRow row = new GenericRow(fieldNum);
			row.setHeader(baseRow.getHeader());
			InternalType[] internalTypes = baseRowTypeInfo.getInternalTypes();
			for (int i = 0; i < fieldNum; i++) {
				if (baseRow.isNullAt(i)) {
					row.setField(i, null);
				} else {
					row.setField(i, TypeGetterSetters.get(baseRow, i, internalTypes[i]));
				}
			}
			return row;
		}
	}

}
