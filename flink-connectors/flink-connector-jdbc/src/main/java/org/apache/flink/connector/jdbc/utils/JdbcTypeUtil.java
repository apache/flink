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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformations;
import org.apache.flink.table.types.utils.DataTypeUtils;

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

/** Utils for jdbc type. */
@Internal
public class JdbcTypeUtil {
    private static final Map<TypeInformation<?>, Integer> TYPE_MAPPING;
    private static final Map<Integer, String> SQL_TYPE_NAMES;

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
        names.put(Types.TIME, "TIME");
        names.put(Types.TIMESTAMP, "TIMESTAMP");
        names.put(Types.DECIMAL, "DECIMAL");
        names.put(Types.BINARY, "BINARY");
        SQL_TYPE_NAMES = Collections.unmodifiableMap(names);
    }

    private JdbcTypeUtil() {}

    public static int typeInformationToSqlType(TypeInformation<?> type) {

        if (TYPE_MAPPING.containsKey(type)) {
            return TYPE_MAPPING.get(type);
        } else if (type instanceof ObjectArrayTypeInfo || type instanceof PrimitiveArrayTypeInfo) {
            return Types.ARRAY;
        } else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    public static String getTypeName(int type) {
        return SQL_TYPE_NAMES.get(type);
    }

    public static String getTypeName(TypeInformation<?> type) {
        return SQL_TYPE_NAMES.get(typeInformationToSqlType(type));
    }

    /**
     * The original table schema may contain generated columns which shouldn't be produced/consumed
     * by TableSource/TableSink. And the original TIMESTAMP/DATE/TIME types uses
     * LocalDateTime/LocalDate/LocalTime as the conversion classes, however, JDBC connector uses
     * Timestamp/Date/Time classes. So that we bridge them to the expected conversion classes.
     */
    public static TableSchema normalizeTableSchema(TableSchema schema) {
        TableSchema.Builder physicalSchemaBuilder = TableSchema.builder();
        schema.getTableColumns()
                .forEach(
                        c -> {
                            if (c.isPhysical()) {
                                final DataType type =
                                        DataTypeUtils.transform(
                                                c.getType(), TypeTransformations.timeToSqlTypes());
                                physicalSchemaBuilder.field(c.getName(), type);
                            }
                        });
        return physicalSchemaBuilder.build();
    }
}
