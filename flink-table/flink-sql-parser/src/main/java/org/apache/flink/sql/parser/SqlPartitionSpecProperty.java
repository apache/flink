/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.NlsString;

import javax.annotation.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Properties of PartitionSpec, a key-value pair with key as component identifier and value as
 * string literal. Different from {@link SqlProperty}, {@link SqlPartitionSpecProperty} allows the
 * value is null.
 */
public class SqlPartitionSpecProperty extends SqlCall {

    /** Use this operator only if you don't have a better one. */
    protected static final SqlOperator OPERATOR = new SqlSpecialOperator("Pair", SqlKind.OTHER);

    private final SqlIdentifier key;
    private final @Nullable SqlNode value;

    public SqlPartitionSpecProperty(SqlIdentifier key, @Nullable SqlNode value, SqlParserPos pos) {
        super(pos);
        this.key = requireNonNull(key, "Pair key is missing");
        this.value = value;
    }

    public SqlIdentifier getKey() {
        return key;
    }

    @Nullable
    public SqlNode getValue() {
        return value;
    }

    public String getKeyString() {
        return key.toString();
    }

    @Nullable
    public String getValueString() {
        return value != null ? ((NlsString) SqlLiteral.value(value)).getValue() : null;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        if (value != null) {
            return ImmutableNullableList.of(key, value);
        } else {
            return ImmutableNullableList.of(key);
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        key.unparse(writer, leftPrec, rightPrec);
        if (value != null) {
            writer.keyword("=");
            value.unparse(writer, leftPrec, rightPrec);
        }
    }
}

// End SqlPartitionSpecProperty.java
