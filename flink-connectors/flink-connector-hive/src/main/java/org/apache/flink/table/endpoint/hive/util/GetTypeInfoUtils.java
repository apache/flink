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

package org.apache.flink.table.endpoint.hive.util;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Arrays;
import java.util.List;

import static java.sql.Types.ARRAY;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.JAVA_OBJECT;
import static java.sql.Types.NULL;
import static java.sql.Types.OTHER;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

/** Get type info rowData list. */
public class GetTypeInfoUtils {
    private static Boolean isNumericType(Integer javaType) {
        return javaType == TINYINT
                || javaType == SMALLINT
                || javaType == INTEGER
                || javaType == BIGINT
                || javaType == FLOAT
                || javaType == DOUBLE
                || javaType == DECIMAL;
    }

    private static RowData toRowData(String name, Integer javaType, Integer precision) {
        return GenericRowData.of(
                BinaryStringData.fromString(name), // TYPE_NAME
                javaType, // DATA_TYPE
                precision, // PRECISION
                null, // LITERAL_PREFIX
                null, // LITERAL_SUFFIX
                null, // CREATE_PARAMS
                Short.valueOf("1"), // NULLABLE
                javaType == VARCHAR, // CASE_SENSITIVE
                javaType < 1111 ? Short.valueOf("3") : Short.valueOf("0"), // SEARCHABLE
                !isNumericType(javaType), // UNSIGNED_ATTRIBUTE
                Boolean.FALSE, // FIXED_PREC_SCALE
                Boolean.FALSE, // AUTO_INCREMENT
                null, // LOCAL_TYPE_NAME
                Short.valueOf("1"), // MINIMUM_SCALE
                Short.valueOf("1"), // MAXIMUM_SCALE
                null, // SQL_DATA_TYPE
                null, // SQL_DATETIME_SUB
                isNumericType(javaType) ? 10 : null); // NUM_PREC_RADIX
    }

    public static List<RowData> getTypeInfoRowDataList() {
        return Arrays.asList(
                toRowData(LogicalTypeRoot.CHAR.name(), CHAR, null),
                toRowData(LogicalTypeRoot.VARCHAR.name(), VARCHAR, null),
                toRowData(LogicalTypeRoot.BOOLEAN.name(), BOOLEAN, null),
                toRowData(LogicalTypeRoot.BINARY.name(), BINARY, null),
                toRowData(LogicalTypeRoot.VARBINARY.name(), VARBINARY, null),
                toRowData(LogicalTypeRoot.DECIMAL.name(), DECIMAL, 38),
                toRowData(LogicalTypeRoot.TINYINT.name(), TINYINT, 3),
                toRowData(LogicalTypeRoot.SMALLINT.name(), SMALLINT, 5),
                toRowData(LogicalTypeRoot.INTEGER.name(), INTEGER, 10),
                toRowData(LogicalTypeRoot.BIGINT.name(), BIGINT, 19),
                toRowData(LogicalTypeRoot.FLOAT.name(), FLOAT, 7),
                toRowData(LogicalTypeRoot.DOUBLE.name(), DOUBLE, 15),
                toRowData(LogicalTypeRoot.DATE.name(), DATE, null),
                toRowData(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE.name(), TIME, null),
                toRowData(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE.name(), TIMESTAMP, null),
                toRowData(
                        LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE.name(),
                        TIMESTAMP_WITH_TIMEZONE,
                        null),
                toRowData(
                        LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE.name(),
                        TIMESTAMP_WITH_TIMEZONE,
                        null),
                toRowData(LogicalTypeRoot.INTERVAL_YEAR_MONTH.name(), OTHER, null),
                toRowData(LogicalTypeRoot.INTERVAL_DAY_TIME.name(), OTHER, null),
                toRowData(LogicalTypeRoot.ARRAY.name(), ARRAY, null),
                toRowData(LogicalTypeRoot.MULTISET.name(), JAVA_OBJECT, null),
                toRowData(LogicalTypeRoot.MAP.name(), JAVA_OBJECT, null),
                toRowData(LogicalTypeRoot.ROW.name(), JAVA_OBJECT, null),
                toRowData(LogicalTypeRoot.DISTINCT_TYPE.name(), OTHER, null),
                toRowData(LogicalTypeRoot.STRUCTURED_TYPE.name(), OTHER, null),
                toRowData(LogicalTypeRoot.NULL.name(), NULL, null),
                toRowData(LogicalTypeRoot.RAW.name(), OTHER, null),
                toRowData(LogicalTypeRoot.SYMBOL.name(), OTHER, null),
                toRowData(LogicalTypeRoot.UNRESOLVED.name(), OTHER, null));
    }
}
