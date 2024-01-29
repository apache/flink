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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/**
 * Distribution statement in CREATE TABLE DDL, e.g. {@code DISTRIBUTED BY HASH(column1, column2)
 * INTO BUCKETS 10}.
 */
public class SqlDistribution extends SqlCall {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("DISTRIBUTED BY", SqlKind.OTHER);

    private final String distributionKind;
    private final SqlNodeList bucketColumns;
    private final SqlNumericLiteral bucketCount;

    public SqlDistribution(
            SqlParserPos pos,
            @Nullable String distributionKind,
            @Nullable SqlNodeList bucketColumns,
            @Nullable SqlNumericLiteral bucketCount) {
        super(pos);
        this.distributionKind = distributionKind;
        this.bucketColumns = bucketColumns;
        this.bucketCount = bucketCount;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(bucketCount, bucketColumns);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.newlineAndIndent();

        if (bucketColumns.size() == 0 && bucketCount != null) {
            writer.keyword("DISTRIBUTED INTO");
            bucketCount.unparse(writer, leftPrec, rightPrec);
            writer.keyword("BUCKETS");
            writer.newlineAndIndent();
            return;
        }

        writer.keyword("DISTRIBUTED BY");
        if (distributionKind != null) {
            writer.print(distributionKind);
        }
        SqlWriter.Frame bucketFrame = writer.startList("(", ")");
        bucketColumns.unparse(writer, leftPrec, rightPrec);
        writer.endList(bucketFrame);

        if (bucketCount != null) {
            writer.keyword("INTO");
            bucketCount.unparse(writer, leftPrec, rightPrec);
            writer.keyword("BUCKETS");
        }
        writer.newlineAndIndent();
    }

    public Optional<String> getDistributionKind() {
        return Optional.ofNullable(distributionKind);
    }

    public SqlNumericLiteral getBucketCount() {
        return bucketCount;
    }

    public SqlNodeList getBucketColumns() {
        return bucketColumns;
    }
}
