/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser.ddl;

import org.apache.calcite.sql.SqlAlter;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.NlsString;

import java.util.Collections;
import java.util.List;

/** Add Jar command to add jar into the classloader. */
public class SqlAddJar extends SqlAlter {

    public static final SqlOperator OPERATOR = new SqlSpecialOperator("ADD JAR", SqlKind.OTHER_DDL);

    private final SqlCharStringLiteral jarPath;

    public SqlAddJar(SqlParserPos pos, SqlCharStringLiteral jarPath) {
        super(pos);
        this.jarPath = jarPath;
    }

    @Override
    protected void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ADD");
        writer.keyword("JAR");
        jarPath.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.singletonList(jarPath);
    }

    public String getPath() {
        return jarPath.getValueAs(NlsString.class).getValue();
    }
}
