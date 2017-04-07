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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * This class enumerates all supported types of
 * the BasicTypeInfo, SqlTimeTypeInfo and RowTypeInfo for creation simplifying
 */
public class Types {

	public static final BasicTypeInfo<String> STRING = BasicTypeInfo.STRING_TYPE_INFO;
	public static final BasicTypeInfo<Boolean> BOOLEAN = BasicTypeInfo.BOOLEAN_TYPE_INFO;
	public static final BasicTypeInfo<Byte> BYTE = BasicTypeInfo.BYTE_TYPE_INFO;
	public static final BasicTypeInfo<Short> SHORT = BasicTypeInfo.SHORT_TYPE_INFO;
	public static final BasicTypeInfo<Integer> INT = BasicTypeInfo.INT_TYPE_INFO;
	public static final BasicTypeInfo<Long> LONG = BasicTypeInfo.LONG_TYPE_INFO;
	public static final BasicTypeInfo<Float> FLOAT = BasicTypeInfo.FLOAT_TYPE_INFO;
	public static final BasicTypeInfo<Double> DOUBLE = BasicTypeInfo.DOUBLE_TYPE_INFO;
	public static final BasicTypeInfo<BigDecimal> DECIMAL = BasicTypeInfo.BIG_DEC_TYPE_INFO;

	public static final SqlTimeTypeInfo<Date> SQL_DATE = SqlTimeTypeInfo.DATE;
	public static final SqlTimeTypeInfo<Time> SQL_TIME = SqlTimeTypeInfo.TIME;
	public static final SqlTimeTypeInfo<Timestamp> SQL_TIMESTAMP = SqlTimeTypeInfo.TIMESTAMP;

	/**
	 * Generates RowTypeInfo with default names (f1, f2 ..).
	 * same as new RowTypeInfo(types)
	 *
	 * @param types of Row fields. e.g. ROW(Types.STRING, Types.INT)
	 */
	public static RowTypeInfo ROW(TypeInformation<?>... types) {
		return new RowTypeInfo(types);
	}

	/**
	 * Generates RowTypeInfo.
	 * same as new RowTypeInfo(types, names)
	 *
	 * e.g. ROW(new String[]{"name", "number"}, Types.STRING, Types.INT)
	 *
	 * @param fieldNames array of field names
	 * @param types array of field types
	 */
	public static RowTypeInfo ROW(String[] fieldNames, TypeInformation<?>... types) {
		return new RowTypeInfo(types, fieldNames);
	}
}
