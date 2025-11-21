/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser;

import org.apache.flink.sql.parser.ddl.SqlTableOption;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.NlsString;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Utils methods for parsing DDLs. */
public class SqlParseUtils {

    private SqlParseUtils() {}

    /**
     * Get static partition key value pair as strings.
     *
     * <p>For character literals we return the unquoted and unescaped values. For other types we use
     * {@link SqlLiteral#toString()} to get the string format of the value literal.
     *
     * @return the mapping of column names to values of partition specifications, returns an empty
     *     map if there is no partition specifications.
     */
    public static LinkedHashMap<String, String> getPartitionKVs(SqlNodeList partitionSpec) {
        if (partitionSpec == null) {
            return null;
        }
        LinkedHashMap<String, String> ret = new LinkedHashMap<>();
        if (partitionSpec.isEmpty()) {
            return ret;
        }
        for (SqlNode node : partitionSpec.getList()) {
            SqlProperty sqlProperty = (SqlProperty) node;
            String value = extractString(sqlProperty.getValue());
            ret.put(sqlProperty.getKey().getSimple(), value);
        }
        return ret;
    }

    @Nullable
    public static String extractString(@Nullable SqlLiteral literal) {
        return literal == null ? null : literal.getValueAs(NlsString.class).getValue();
    }

    @Nullable
    public static String extractString(@Nullable SqlNode node) {
        if (node == null) {
            return null;
        }
        final Comparable value = SqlLiteral.value(node);
        if (value == null) {
            return null;
        }
        return value instanceof NlsString ? ((NlsString) value).getValue() : value.toString();
    }

    public static Map<String, String> extractMap(@Nullable SqlNodeList propList) {
        if (propList == null) {
            return Map.of();
        }
        return propList.getList().stream()
                .map(p -> (SqlTableOption) p)
                .collect(Collectors.toMap(k -> k.getKeyString(), SqlTableOption::getValueString));
    }

    public static List<String> extractList(@Nullable SqlNodeList sqlNodeList) {
        if (sqlNodeList == null) {
            return List.of();
        }
        return sqlNodeList.getList().stream()
                .map(p -> ((SqlIdentifier) p).getSimple())
                .collect(Collectors.toList());
    }
}
