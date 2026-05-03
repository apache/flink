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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Optional;

/**
 * SqlNode to describe the ALTER MATERIALIZED TABLE [catalogName.][dataBasesName.]tableName
 * (ADD|MODIFY) DISTRIBUTION statement.
 */
public abstract class SqlAlterMaterializedTableDistribution extends SqlAlterMaterializedTable {
    protected final SqlDistribution distribution;

    public SqlAlterMaterializedTableDistribution(
            SqlParserPos pos, SqlIdentifier tableName, SqlDistribution distribution) {
        super(pos, tableName);
        this.distribution = distribution;
    }

    protected abstract String getAlterOperation();

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, distribution);
    }

    @Override
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparseAlterOperation(writer, leftPrec, rightPrec);
        writer.keyword(getAlterOperation());
        distribution.unparseAlter(writer, leftPrec, rightPrec);
    }

    public Optional<SqlDistribution> getDistribution() {
        return Optional.ofNullable(distribution);
    }

    public static class SqlAlterMaterializedTableAddDistribution
            extends SqlAlterMaterializedTableDistribution {

        public SqlAlterMaterializedTableAddDistribution(
                SqlParserPos pos, SqlIdentifier tableName, SqlDistribution distribution) {
            super(pos, tableName, distribution);
        }

        @Override
        protected String getAlterOperation() {
            return "ADD";
        }
    }

    public static class SqlAlterMaterializedTableModifyDistribution
            extends SqlAlterMaterializedTableDistribution {

        public SqlAlterMaterializedTableModifyDistribution(
                SqlParserPos pos, SqlIdentifier tableName, SqlDistribution distribution) {
            super(pos, tableName, distribution);
        }

        @Override
        protected String getAlterOperation() {
            return "MODIFY";
        }
    }
}
