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

package org.apache.flink.sql.parser.ddl.materializedtable;

import org.apache.flink.sql.parser.ddl.SqlDistribution;
import org.apache.flink.sql.parser.ddl.SqlRefreshMode;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;

import java.util.List;

/** CREATE [OR ALTER] MATERIALIZED TABLE DDL sql call. */
public class SqlCreateOrAlterMaterializedTable extends SqlCreateMaterializedTable {

    public static final SqlSpecialOperator CREATE_OR_ALTER_OPERATOR =
            new SqlSpecialOperator("CREATE OR ALTER MATERIALIZED TABLE", SqlKind.OTHER_DDL);

    public SqlCreateOrAlterMaterializedTable(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList columnList,
            List<SqlTableConstraint> tableConstraints,
            SqlWatermark watermark,
            @Nullable SqlCharStringLiteral comment,
            @Nullable SqlDistribution distribution,
            SqlNodeList partitionKeyList,
            SqlNodeList propertyList,
            @Nullable SqlIntervalLiteral freshness,
            @Nullable SqlRefreshMode refreshMode,
            SqlNode asQuery,
            boolean isOrAlter) {
        super(
                isOrAlter ? CREATE_OR_ALTER_OPERATOR : CREATE_OPERATOR,
                pos,
                tableName,
                columnList,
                tableConstraints,
                watermark,
                comment,
                distribution,
                partitionKeyList,
                propertyList,
                freshness,
                refreshMode,
                asQuery);
    }

    @Override
    protected void unparseCreateIfNotExists(SqlWriter writer, int leftPrec, int rightPrec) {
        if (getOperator() == CREATE_OPERATOR) {
            super.unparseCreateIfNotExists(writer, leftPrec, rightPrec);
            return;
        }

        writer.keyword("CREATE OR ALTER");
        if (isTemporary()) {
            writer.keyword("TEMPORARY");
        }
        writer.keyword(getScope());
        if (isIfNotExists()) {
            writer.keyword("IF NOT EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);
    }
}
