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

package org.apache.flink.connector.jdbc.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.LocalTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

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
import static org.apache.flink.table.types.logical.LogicalTypeRoot.ARRAY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BINARY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BOOLEAN;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.CHAR;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DATE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DOUBLE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.FLOAT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTEGER;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.SMALLINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TINYINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARBINARY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR;

/** Utils for jdbc type. */
@Internal
public class JdbcTypeUtil {

    private static final Map<TypeInformation<?>, Integer> TYPE_MAPPING;

    static {
        HashMap<TypeInformation<?>, Integer> m = new HashMap<>();
        m.put(STRING_TYPE_INFO, Types.VARCHAR);
        m.put(BOOLEAN_TYPE_INFO, Types.BOOLEAN);
        m.put(BYTE_TYPE_INFO, Types.TINYINT);
        m.put(SHORT_TYPE_INFO, Types.SMALLINT);
        m.put(INT_TYPE_INFO, Types.INTEGER);
        m.put(LONG_TYPE_INFO, Types.BIGINT);
        m.put(FLOAT_TYPE_INFO, Types.REAL);
        m.put(DOUBLE_TYPE_INFO, Types.DOUBLE);
        m.put(SqlTimeTypeInfo.DATE, Types.DATE);
        m.put(SqlTimeTypeInfo.TIME, Types.TIME);
        m.put(SqlTimeTypeInfo.TIMESTAMP, Types.TIMESTAMP);
        m.put(LocalTimeTypeInfo.LOCAL_DATE, Types.DATE);
        m.put(LocalTimeTypeInfo.LOCAL_TIME, Types.TIME);
        m.put(LocalTimeTypeInfo.LOCAL_DATE_TIME, Types.TIMESTAMP);
        m.put(BIG_DEC_TYPE_INFO, Types.DECIMAL);
        m.put(BYTE_PRIMITIVE_ARRAY_TYPE_INFO, Types.BINARY);
        TYPE_MAPPING = Collections.unmodifiableMap(m);
    }

    private static final Map<LogicalTypeRoot, Integer> LOGICAL_TYPE_MAPPING =
            ImmutableMap.<LogicalTypeRoot, Integer>builder()
                    .put(VARCHAR, Types.VARCHAR)
                    .put(CHAR, Types.CHAR)
                    .put(VARBINARY, Types.VARBINARY)
                    .put(BOOLEAN, Types.BOOLEAN)
                    .put(BINARY, Types.BINARY)
                    .put(TINYINT, Types.TINYINT)
                    .put(SMALLINT, Types.SMALLINT)
                    .put(INTEGER, Types.INTEGER)
                    .put(BIGINT, Types.BIGINT)
                    .put(FLOAT, Types.REAL)
                    .put(DOUBLE, Types.DOUBLE)
                    .put(DATE, Types.DATE)
                    .put(TIMESTAMP_WITHOUT_TIME_ZONE, Types.TIMESTAMP)
                    .put(TIMESTAMP_WITH_TIME_ZONE, Types.TIMESTAMP_WITH_TIMEZONE)
                    .put(TIME_WITHOUT_TIME_ZONE, Types.TIME)
                    .put(DECIMAL, Types.DECIMAL)
                    .put(ARRAY, Types.ARRAY)
                    .build();

    private JdbcTypeUtil() {}

    /** Drop this method once Python is not using JdbcOutputFormat. */
    @Deprecated
    public static int typeInformationToSqlType(TypeInformation<?> type) {
        if (TYPE_MAPPING.containsKey(type)) {
            return TYPE_MAPPING.get(type);
        } else if (type instanceof ObjectArrayTypeInfo || type instanceof PrimitiveArrayTypeInfo) {
            return Types.ARRAY;
        } else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    public static int logicalTypeToSqlType(LogicalTypeRoot typeRoot) {
        if (LOGICAL_TYPE_MAPPING.containsKey(typeRoot)) {
            return LOGICAL_TYPE_MAPPING.get(typeRoot);
        } else {
            throw new IllegalArgumentException("Unsupported typeRoot: " + typeRoot);
        }
    }
}
