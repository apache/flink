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

package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.SqlPartitionUtils;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;

import java.util.LinkedHashMap;
import java.util.List;

/** ANALYZE TABLE to compute the statistics for a given table. */
public class SqlAnalyzeTable extends SqlCall {
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("ANALYZE TABLE", SqlKind.OTHER_DDL);

    private final SqlIdentifier tableName;
    private final SqlNodeList partitions;
    private final SqlNodeList columns;
    private final boolean allColumns;

    public SqlAnalyzeTable(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList partitions,
            SqlNodeList columns,
            boolean allColumns) {
        super(pos);
        this.tableName = tableName;
        this.partitions = partitions;
        this.columns = columns;
        this.allColumns = allColumns;
    }

    public String[] fullTableName() {
        return tableName.names.toArray(new String[0]);
    }

    /** Get partition spec as key-value strings. */
    public LinkedHashMap<String, String> getPartitionKVs() {
        return SqlPartitionUtils.getPartitionKVs(partitions);
    }

    public String[] getColumnNames() {
        if (columns == null) {
            return new String[0];
        }
        return columns.getList().stream()
                .map(col -> ((SqlIdentifier) col).getSimple())
                .toArray(String[]::new);
    }

    public boolean isAllColumns() {
        return allColumns;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(tableName, partitions, columns);
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ANALYZE");
        writer.keyword("TABLE");
        final int opLeft = getOperator().getLeftPrec();
        final int opRight = getOperator().getRightPrec();
        tableName.unparse(writer, opLeft, opRight);

        if (partitions != null && partitions.size() > 0) {
            writer.keyword("PARTITION");
            partitions.unparse(writer, opLeft, opRight);
        }

        writer.keyword("COMPUTE");
        writer.keyword("STATISTICS");

        if (columns != null && columns.size() > 0) {
            writer.keyword("FOR");
            writer.keyword("COLUMNS");
            columns.unparse(writer, opLeft, opRight);
        }
        if (allColumns) {
            writer.keyword("FOR");
            writer.keyword("ALL");
            writer.keyword("COLUMNS");
        }
    }
}
