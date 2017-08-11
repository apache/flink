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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BIG_DEC_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BIG_INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BOOLEAN_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BYTE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.CHAR_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.DATE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.FLOAT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.SHORT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;

class JDBCTypeUtil {
	private static final Map<BasicTypeInfo<?>, Integer> BASIC_TYPES;
	private static final Map<Integer, String> SQL_TYPE_NAMES;

	static {
		HashMap<BasicTypeInfo<?>, Integer> m = new HashMap<>();
		m.put(STRING_TYPE_INFO, Types.VARCHAR);
		m.put(BOOLEAN_TYPE_INFO, Types.BOOLEAN);
		m.put(BYTE_TYPE_INFO, Types.TINYINT);
		m.put(SHORT_TYPE_INFO, Types.SMALLINT);
		m.put(INT_TYPE_INFO, Types.INTEGER);
		m.put(LONG_TYPE_INFO, Types.BIGINT);
		m.put(FLOAT_TYPE_INFO, Types.FLOAT);
		m.put(DOUBLE_TYPE_INFO, Types.DOUBLE);
		m.put(CHAR_TYPE_INFO, Types.SMALLINT);
		m.put(DATE_TYPE_INFO, Types.DATE);
		m.put(BIG_INT_TYPE_INFO, Types.DECIMAL);
		m.put(BIG_DEC_TYPE_INFO, Types.DECIMAL);
		BASIC_TYPES = Collections.unmodifiableMap(m);

		HashMap<Integer, String> names = new HashMap<>();
		names.put(Types.VARCHAR, "VARCHAR");
		names.put(Types.BOOLEAN, "BOOLEAN");
		names.put(Types.TINYINT, "TINYINT");
		names.put(Types.SMALLINT, "SMALLINT");
		names.put(Types.INTEGER, "INTEGER");
		names.put(Types.BIGINT, "BIGINT");
		names.put(Types.FLOAT, "FLOAT");
		names.put(Types.DOUBLE, "DOUBLE");
		names.put(Types.CHAR, "CHAR");
		names.put(Types.DATE, "DATE");
		names.put(Types.DECIMAL, "DECIMAL");
		SQL_TYPE_NAMES = Collections.unmodifiableMap(names);
	}

	private JDBCTypeUtil() {
	}

	static int typeInformationToSqlType(TypeInformation<?> type) {
		if (type.isBasicType()) {
			return basicTypeToSqlType((BasicTypeInfo<?>) type);
		} else if (type instanceof ObjectArrayTypeInfo || type instanceof PrimitiveArrayTypeInfo) {
			return Types.ARRAY;
		} else if (type instanceof CompositeType) {
			return Types.STRUCT;
		} else {
			throw new IllegalArgumentException("Unsupported type " + type);
		}
	}

	static String getTypeName(int type) {
		return SQL_TYPE_NAMES.get(type);
	}

	private static int basicTypeToSqlType(BasicTypeInfo<?> type) {
		Integer o = BASIC_TYPES.get(type);
		if (o == null) {
			throw new IllegalArgumentException("Unsupported type " + type);
		}
		return o;
	}
}
