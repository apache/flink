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

import org.apache.flink.sql.parser.SqlParseUtils;
import org.apache.flink.sql.parser.ddl.table.SqlAlterTable;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/** ALTER TABLE DDL to drop partitions of a table. */
public class SqlDropPartitions extends SqlAlterTable {

    private static final SqlSpecialOperator DROP_PARTITION_OPERATOR =
            new SqlSpecialOperator("ALTER TABLE DROP PARTITION", SqlKind.ALTER_TABLE) {
                @Override
                public SqlCall createCall(
                        SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
                    List<SqlNodeList> partSpecs = new ArrayList<>();
                    for (SqlNode spec : (SqlNodeList) operands[2]) {
                        partSpecs.add((SqlNodeList) spec);
                    }
                    return new SqlDropPartitions(
                            pos,
                            (SqlIdentifier) operands[0],
                            ((SqlLiteral) operands[1]).booleanValue(),
                            partSpecs);
                }
            };

    private final boolean ifExists;
    private final List<SqlNodeList> partSpecs;

    public SqlDropPartitions(
            SqlParserPos pos,
            SqlIdentifier tableName,
            boolean ifExists,
            List<SqlNodeList> partSpecs) {
        super(pos, tableName, false);
        this.ifExists = ifExists;
        this.partSpecs = partSpecs;
    }

    public boolean ifExists() {
        return ifExists;
    }

    public List<SqlNodeList> getPartSpecs() {
        return partSpecs;
    }

    public LinkedHashMap<String, String> getPartitionKVs(int i) {
        return SqlParseUtils.getPartitionKVs(getPartSpecs().get(i));
    }

    @Override
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparseAlterOperation(writer, leftPrec, rightPrec);
        writer.newlineAndIndent();
        writer.keyword("DROP");
        if (ifExists) {
            writer.keyword("IF EXISTS");
        }
        int opLeftPrec = getOperator().getLeftPrec();
        int opRightPrec = getOperator().getRightPrec();
        final SqlWriter.Frame frame = writer.startList("", "");
        for (SqlNodeList partSpec : partSpecs) {
            writer.sep(",");
            writer.newlineAndIndent();
            writer.keyword("PARTITION");
            partSpec.unparse(writer, opLeftPrec, opRightPrec);
        }
        writer.endList(frame);
    }

    @Override
    public SqlOperator getOperator() {
        return DROP_PARTITION_OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return List.of(
                tableIdentifier,
                SqlLiteral.createBoolean(ifExists, SqlParserPos.ZERO),
                new SqlNodeList(partSpecs, SqlParserPos.ZERO));
    }

    /** Alter table add partition context. */
    public static class AlterTableDropPartitionsContext {
        public boolean ifExists;
        public List<SqlNodeList> partSpecs;
    }
}
