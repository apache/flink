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

import org.apache.flink.annotation.Internal;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.NlsString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

/** SQL call for "SET" and "SET 'key' = 'value'". */
@Internal
public class SqlSet extends SqlCall {

    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("SET", SqlKind.OTHER);

    @Nullable private final SqlNode key;
    @Nullable private final SqlNode value;

    public SqlSet(SqlParserPos pos, SqlNode key, SqlNode value) {
        super(pos);
        this.key = Objects.requireNonNull(key, "key cannot be null");
        this.value = Objects.requireNonNull(value, "value cannot be null");
    }

    public SqlSet(SqlParserPos pos) {
        super(pos);
        this.key = null;
        this.value = null;
    }

    @Override
    @Nonnull
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    @Nonnull
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(key, value);
    }

    public @Nullable SqlNode getKey() {
        return key;
    }

    public @Nullable SqlNode getValue() {
        return value;
    }

    public @Nullable String getKeyString() {
        if (key == null) {
            return null;
        }

        return ((NlsString) SqlLiteral.value(key)).getValue();
    }

    public @Nullable String getValueString() {
        if (value == null) {
            return null;
        }

        return ((NlsString) SqlLiteral.value(value)).getValue();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SET");

        if (key != null && value != null) {
            key.unparse(writer, leftPrec, rightPrec);
            writer.keyword("=");
            value.unparse(writer, leftPrec, rightPrec);
        }
    }
}
