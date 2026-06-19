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

package org.apache.flink.sql.parser.ddl.model;

import org.apache.flink.sql.parser.ddl.SqlAlterObject;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Abstract class to describe statements like ALTER MODEL [IF EXISTS] [[catalogName.]
 * dataBasesName.]modelName ...
 */
public abstract class SqlAlterModel extends SqlAlterObject {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("ALTER MODEL", SqlKind.OTHER_DDL);

    protected final boolean ifModelExists;

    public SqlAlterModel(SqlParserPos pos, SqlIdentifier modelName, boolean ifModelExists) {
        super(OPERATOR, pos, "MODEL", modelName);
        this.ifModelExists = ifModelExists;
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
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        if (ifModelExists) {
            writer.keyword("IF EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);
    }
}
