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

import org.apache.flink.sql.parser.SqlPartitionUtils;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;

import java.util.LinkedHashMap;
import java.util.List;

import static java.util.Objects.requireNonNull;

/** ALTER TABLE DDL to change the specifications of a partition. */
public class SqlAlterPartitionRename extends SqlAlterTable {

    private final SqlNodeList newPartSpec;

    public SqlAlterPartitionRename(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList partSpec,
            SqlNodeList newPartSpec) {
        super(
                pos,
                tableName,
                requireNonNull(
                        partSpec, "Old partition spec have to be specified when rename partition"));
        this.newPartSpec = newPartSpec;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(tableIdentifier, getPartitionSpec(), newPartSpec);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.newlineAndIndent();
        writer.keyword("RENAME TO");
        writer.newlineAndIndent();
        writer.keyword("PARTITION");
        newPartSpec.unparse(writer, getOperator().getLeftPrec(), getOperator().getRightPrec());
    }

    public SqlNodeList getNewPartSpec() {
        return newPartSpec;
    }

    /** Get partition spec as key-value strings. */
    public LinkedHashMap<String, String> getNewPartitionKVs() {
        return SqlPartitionUtils.getPartitionKVs(getNewPartSpec());
    }
}
