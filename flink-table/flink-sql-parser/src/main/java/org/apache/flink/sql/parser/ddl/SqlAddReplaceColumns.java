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
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;

/** ALTER DDL to ADD or REPLACE columns for a table. */
public class SqlAddReplaceColumns extends SqlAlterTable {

    private final SqlNodeList newColumns;
    // Whether to replace all the existing columns. If false, new columns will be appended to the
    // end of the schema.
    private final boolean replace;
    // properties that should be added to the table
    private final SqlNodeList properties;

    public SqlAddReplaceColumns(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList newColumns,
            boolean replace,
            @Nullable SqlNodeList properties) {
        super(pos, tableName);
        this.newColumns = newColumns;
        this.replace = replace;
        this.properties = properties;
    }

    public SqlNodeList getNewColumns() {
        return newColumns;
    }

    public boolean isReplace() {
        return replace;
    }

    public SqlNodeList getProperties() {
        return properties;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(tableIdentifier, partitionSpec, newColumns, properties);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        if (replace) {
            writer.keyword("REPLACE");
        } else {
            writer.keyword("ADD");
        }
        writer.keyword("COLUMNS");
        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
        for (SqlNode column : newColumns) {
            printIndent(writer);
            column.unparse(writer, leftPrec, rightPrec);
        }
        writer.newlineAndIndent();
        writer.endList(frame);
    }

    protected void printIndent(SqlWriter writer) {
        writer.sep(",", false);
        writer.newlineAndIndent();
        writer.print("  ");
    }
}
