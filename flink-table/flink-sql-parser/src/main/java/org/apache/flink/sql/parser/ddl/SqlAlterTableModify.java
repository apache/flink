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

package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.SqlUnparseUtils;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;

import java.util.List;

/**
 * SqlNode to describe ALTER TABLE [IF EXISTS] table_name MODIFY column/constraint/watermark clause.
 *
 * <p>Example: DDL like the below for modify column/constraint/watermark.
 *
 * <pre>{@code
 * -- modify single column
 * ALTER TABLE mytable MODIFY new_column STRING COMMENT 'new_column docs';
 *
 * -- modify multiple columns, constraint, and watermark
 * ALTER TABLE mytable MODIFY (
 *     log_ts STRING COMMENT 'log timestamp string' FIRST,
 *     ts AS TO_TIMESTAMP(log_ts) AFTER log_ts,
 *     col_meta int metadata from 'mk1' virtual AFTER col_b,
 *     PRIMARY KEY (id) NOT ENFORCED,
 *     WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
 * );
 * }</pre>
 */
public class SqlAlterTableModify extends SqlAlterTableSchema {

    public SqlAlterTableModify(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList modifiedColumns,
            List<SqlTableConstraint> constraints,
            @Nullable SqlWatermark watermark,
            boolean ifTableExists) {
        super(pos, tableName, modifiedColumns, constraints, watermark, ifTableExists);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("MODIFY");
        // unparse table schema
        SqlUnparseUtils.unparseTableSchema(
                writer, leftPrec, rightPrec, columnList, constraints, watermark);
    }
}
