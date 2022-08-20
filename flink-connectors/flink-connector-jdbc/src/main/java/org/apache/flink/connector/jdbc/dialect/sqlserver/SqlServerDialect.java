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

package org.apache.flink.connector.jdbc.dialect.sqlserver;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.internal.converter.SqlServerRowConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** JDBC dialect for SqlServer. */
@Internal
public class SqlServerDialect extends AbstractDialect {
    @Override
    public String dialectName() {
        return "SqlServer";
    }

    /**
     * The maximum precision is supported by datetime2.
     * https://docs.microsoft.com/en-us/sql/t-sql/data-types/datetime2-transact-sql?view=sql-server-ver16
     */
    @Override
    public Optional<Range> timestampPrecisionRange() {
        return Optional.of(Range.of(0, 7));
    }

    /**
     * The maximum precision is supported by decimal.
     * https://docs.microsoft.com/en-us/sql/t-sql/data-types/precision-scale-and-length-transact-sql?view=sql-server-ver16
     */
    @Override
    public Optional<Range> decimalPrecisionRange() {
        return Optional.of(Range.of(0, 38));
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return identifier;
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        List<String> nonUniqueKeyFields =
                Arrays.stream(fieldNames)
                        .filter(f -> !Arrays.asList(uniqueKeyFields).contains(f))
                        .collect(Collectors.toList());
        String fieldsProjection =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));

        String valuesBinding =
                Arrays.stream(fieldNames)
                        .map(f -> ":" + f + " " + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));

        String usingClause = String.format("SELECT %s", valuesBinding);
        String onConditions =
                Arrays.stream(uniqueKeyFields)
                        .map(
                                f ->
                                        "[TARGET]."
                                                + quoteIdentifier(f)
                                                + "=[SOURCE]."
                                                + quoteIdentifier(f))
                        .collect(Collectors.joining(" AND "));
        String updateSetClause =
                nonUniqueKeyFields.stream()
                        .map(
                                f ->
                                        "[TARGET]."
                                                + quoteIdentifier(f)
                                                + "=[SOURCE]."
                                                + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));

        String insertValues =
                Arrays.stream(fieldNames)
                        .map(f -> "[SOURCE]." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));
        return Optional.of(
                String.format(
                        "MERGE INTO %s AS [TARGET]"
                                + " USING (%s) AS [SOURCE]"
                                + " ON (%s)"
                                + " WHEN MATCHED THEN"
                                + " UPDATE SET %s"
                                + " WHEN NOT MATCHED THEN"
                                + " INSERT (%s) VALUES (%s);",
                        quoteIdentifier(tableName),
                        usingClause,
                        onConditions,
                        updateSetClause,
                        fieldsProjection,
                        insertValues));
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new SqlServerRowConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        throw new IllegalArgumentException("SqlServerDialect does not support limit clause");
    }

    @Override
    public Set<LogicalTypeRoot> supportedTypes() {
        return EnumSet.of(
                LogicalTypeRoot.CHAR,
                LogicalTypeRoot.VARCHAR,
                LogicalTypeRoot.BOOLEAN,
                LogicalTypeRoot.BINARY,
                LogicalTypeRoot.VARBINARY,
                LogicalTypeRoot.DECIMAL,
                LogicalTypeRoot.TINYINT,
                LogicalTypeRoot.SMALLINT,
                LogicalTypeRoot.INTEGER,
                LogicalTypeRoot.BIGINT,
                LogicalTypeRoot.FLOAT,
                LogicalTypeRoot.DOUBLE,
                LogicalTypeRoot.DATE,
                LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    }
}
