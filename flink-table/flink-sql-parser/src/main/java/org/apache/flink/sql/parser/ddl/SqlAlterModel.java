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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import static java.util.Objects.requireNonNull;

/**
 * Abstract class to describe statements like ALTER MODEL [IF EXISTS] [[catalogName.]
 * dataBasesName.]modelName ...
 */
public abstract class SqlAlterModel extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("ALTER MODEL", SqlKind.OTHER_DDL);

    protected final SqlIdentifier modelName;
    protected final boolean ifModelExists;

    public SqlAlterModel(SqlParserPos pos, SqlIdentifier modelName, boolean ifModelExists) {
        super(pos);
        this.modelName = requireNonNull(modelName, "modelName should not be null");
        this.ifModelExists = ifModelExists;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public SqlIdentifier getModelName() {
        return modelName;
    }

    public String[] fullModelName() {
        return modelName.names.toArray(new String[0]);
    }

    /**
     * Whether to ignore the error if the model doesn't exist.
     *
     * @return true when IF EXISTS is specified.
     */
    public boolean ifModelExists() {
        return ifModelExists;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER MODEL");
        if (ifModelExists) {
            writer.keyword("IF EXISTS");
        }
        modelName.unparse(writer, leftPrec, rightPrec);
    }
}
