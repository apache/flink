/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.sql.parser.ddl.table;

import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * SqlNode to describe ALTER TABLE [IF EXISTS] table_name ADD column/constraint/watermark clause.
 *
 * <p>Example: DDL like the below for adding column(s)/constraint/watermark.
 *
 * <pre>{@code
 * -- add single column
 * ALTER TABLE mytable ADD new_column STRING COMMENT 'new_column docs';
 *
 * -- add multiple columns, constraint, and watermark
 * ALTER TABLE mytable ADD (
 *     log_ts STRING COMMENT 'log timestamp string' FIRST,
 *     ts AS TO_TIMESTAMP(log_ts) AFTER log_ts,
 *     col_meta INT METADATA FROM 'mk1' VIRTUAL AFTER col_b,
 *     PRIMARY KEY (id) NOT ENFORCED,
 *     WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
 * );
 * }</pre>
 */
public class SqlAlterTableAdd extends SqlAlterTableSchema {

    private static final SqlSpecialOperator ADD_OPERATOR =
            new SqlSpecialOperator("ALTER TABLE ADD", SqlKind.ALTER_TABLE) {
                @Override
                public SqlCall createCall(
                        SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
                    List<SqlTableConstraint> constraints = new ArrayList<>();
                    for (SqlNode c : (SqlNodeList) operands[2]) {
                        constraints.add((SqlTableConstraint) c);
                    }
                    return new SqlAlterTableAdd(
                            pos,
                            (SqlIdentifier) operands[0],
                            (SqlNodeList) operands[1],
                            constraints,
                            (SqlWatermark) operands[3],
                            ((SqlLiteral) operands[4]).booleanValue());
                }
            };

    public SqlAlterTableAdd(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList addedColumns,
            List<SqlTableConstraint> constraint,
            @Nullable SqlWatermark sqlWatermark,
            boolean ifTableExists) {
        super(pos, tableName, addedColumns, constraint, sqlWatermark, ifTableExists);
    }

    @Override
    public SqlOperator getOperator() {
        return ADD_OPERATOR;
    }

    @Override
    protected String getAlterOperation() {
        return "ADD";
    }
}
