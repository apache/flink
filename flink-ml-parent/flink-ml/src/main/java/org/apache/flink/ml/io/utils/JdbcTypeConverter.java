/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.io.utils;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BIG_DEC_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BOOLEAN_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BYTE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.FLOAT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.SHORT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

/**
 * Jdbc Type Converter.
 */
public class JdbcTypeConverter {
	private static final Map <Integer, TypeInformation <?>> MAP_INDEX_TO_FLINKTYPE;
	private static final Map <String, Integer> MAP_SQLTYPE_TO_INDEX;
	private static final Map <TypeInformation <?>, Integer> MAP_FLINKTYPE_TO_INDEX;
	private static final Map <Integer, String> MAP_INDEX_TO_SQLTYPE;

	static {
		HashMap <TypeInformation <?>, Integer> m1 = new HashMap <>();
		m1.put(STRING_TYPE_INFO, Types.VARCHAR);
		m1.put(BOOLEAN_TYPE_INFO, Types.BOOLEAN);
		m1.put(BYTE_TYPE_INFO, Types.TINYINT);
		m1.put(SHORT_TYPE_INFO, Types.SMALLINT);
		m1.put(INT_TYPE_INFO, Types.INTEGER);
		m1.put(LONG_TYPE_INFO, Types.BIGINT);
		m1.put(FLOAT_TYPE_INFO, Types.FLOAT);
		m1.put(DOUBLE_TYPE_INFO, Types.DOUBLE);
		m1.put(SqlTimeTypeInfo.DATE, Types.DATE);
		m1.put(SqlTimeTypeInfo.TIME, Types.TIME);
		m1.put(SqlTimeTypeInfo.TIMESTAMP, Types.TIMESTAMP);
		m1.put(BIG_DEC_TYPE_INFO, Types.DECIMAL);
		m1.put(BYTE_PRIMITIVE_ARRAY_TYPE_INFO, Types.BINARY);
		MAP_FLINKTYPE_TO_INDEX = Collections.unmodifiableMap(m1);

		HashMap <Integer, String> m2 = new HashMap <>();
		m2.put(Types.LONGVARCHAR, "LONGVARCHAR");
		m2.put(Types.VARCHAR, "VARCHAR");
		m2.put(Types.BOOLEAN, "BOOLEAN");
		m2.put(Types.TINYINT, "TINYINT");
		m2.put(Types.SMALLINT, "SMALLINT");
		m2.put(Types.INTEGER, "INTEGER");
		m2.put(Types.BIGINT, "BIGINT");
		m2.put(Types.FLOAT, "FLOAT");
		m2.put(Types.DOUBLE, "DOUBLE");
		m2.put(Types.CHAR, "CHAR");
		m2.put(Types.DATE, "DATE");
		m2.put(Types.TIME, "TIME");
		m2.put(Types.TIMESTAMP, "TIMESTAMP");
		m2.put(Types.DECIMAL, "DECIMAL");
		m2.put(Types.BINARY, "BINARY");
		MAP_INDEX_TO_SQLTYPE = Collections.unmodifiableMap(m2);

		HashMap <Integer, TypeInformation <?>> m3 = new HashMap <>();
		m3.put(Types.LONGVARCHAR, STRING_TYPE_INFO);
		m3.put(Types.VARCHAR, STRING_TYPE_INFO);
		m3.put(Types.NULL, STRING_TYPE_INFO);
		m3.put(Types.BOOLEAN, BOOLEAN_TYPE_INFO);
		m3.put(Types.TINYINT, BYTE_TYPE_INFO);
		m3.put(Types.SMALLINT, SHORT_TYPE_INFO);
		m3.put(Types.INTEGER, INT_TYPE_INFO);
		m3.put(Types.BIGINT, LONG_TYPE_INFO);
		m3.put(Types.FLOAT, FLOAT_TYPE_INFO);
		m3.put(Types.DOUBLE, DOUBLE_TYPE_INFO);
		m3.put(Types.DATE, SqlTimeTypeInfo.DATE);
		m3.put(Types.TIME, SqlTimeTypeInfo.TIME);
		m3.put(Types.TIMESTAMP, SqlTimeTypeInfo.TIMESTAMP);
		m3.put(Types.DECIMAL, BIG_DEC_TYPE_INFO);
		m3.put(Types.BINARY, BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
		MAP_INDEX_TO_FLINKTYPE = Collections.unmodifiableMap(m3);

		HashMap <String, Integer> m4 = new HashMap <>();
		m4.put("LONGVARCHAR", Types.LONGVARCHAR);
		m4.put("VARCHAR", Types.VARCHAR);
		m4.put("BOOLEAN", Types.BOOLEAN);
		m4.put("TINYINT", Types.TINYINT);
		m4.put("SMALLINT", Types.SMALLINT);
		m4.put("INTEGER", Types.INTEGER);
		m4.put("BIGINT", Types.BIGINT);
		m4.put("FLOAT", Types.FLOAT);
		m4.put("DOUBLE", Types.DOUBLE);
		m4.put("CHAR", Types.CHAR);
		m4.put("DATE", Types.DATE);
		m4.put("TIME", Types.TIME);
		m4.put("TIMESTAMP", Types.TIMESTAMP);
		m4.put("DECIMAL", Types.DECIMAL);
		m4.put("BINARY", Types.BINARY);
		MAP_SQLTYPE_TO_INDEX = Collections.unmodifiableMap(m4);
	}

	static String getSqlType(int type) {
		return MAP_INDEX_TO_SQLTYPE.get(type);
	}

	public static int getIntegerSqlType(TypeInformation <?> type) {

		if (MAP_FLINKTYPE_TO_INDEX.containsKey(type)) {
			return MAP_FLINKTYPE_TO_INDEX.get(type);
		} else if (type instanceof ObjectArrayTypeInfo || type instanceof PrimitiveArrayTypeInfo) {
			return Types.ARRAY;
		} else {
			throw new IllegalArgumentException("Unsupported type: " + type);
		}
	}

	public static String getSqlType(TypeInformation <?> type) {
		return MAP_INDEX_TO_SQLTYPE.get(MAP_FLINKTYPE_TO_INDEX.get(type));
	}

	public static TypeInformation <?> getFlinkType(int typeIndex) {
		return MAP_INDEX_TO_FLINKTYPE.get(typeIndex);
	}

	public static TypeInformation <?> getFlinkType(String typeSQL) {
		return MAP_INDEX_TO_FLINKTYPE.get(MAP_SQLTYPE_TO_INDEX.get(typeSQL.toUpperCase()));
	}

}
