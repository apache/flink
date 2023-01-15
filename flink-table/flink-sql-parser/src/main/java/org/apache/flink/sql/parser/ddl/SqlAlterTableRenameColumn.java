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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * ALTER TABLE [IF EXISTS] [[catalogName.] dataBasesName].tableName RENAME originColumnName TO
 * newColumnName.
 */
public class SqlAlterTableRenameColumn extends SqlAlterTable {

    private final SqlIdentifier originColumnIdentifier;
    private final SqlIdentifier newColumnIdentifier;

    public SqlAlterTableRenameColumn(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlIdentifier originColumnIdentifier,
            SqlIdentifier newColumnIdentifier,
            boolean ifTableExists) {
        super(pos, tableName, null, ifTableExists);
        this.originColumnIdentifier = originColumnIdentifier;
        this.newColumnIdentifier = newColumnIdentifier;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                tableIdentifier, originColumnIdentifier, newColumnIdentifier);
    }

    public SqlIdentifier getOldColumnIdentifier() {
        return originColumnIdentifier;
    }

    public SqlIdentifier getNewColumnIdentifier() {
        return newColumnIdentifier;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("RENAME");
        originColumnIdentifier.unparse(writer, leftPrec, rightPrec);
        writer.keyword("TO");
        newColumnIdentifier.unparse(writer, leftPrec, rightPrec);
    }
}
