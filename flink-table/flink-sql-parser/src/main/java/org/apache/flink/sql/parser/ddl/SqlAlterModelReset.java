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

import org.apache.flink.sql.parser.SqlUnparseUtils;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.NlsString;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * ALTER MODEL [IF EXISTS] [[catalogName.] dataBasesName.]modelName RESET ( 'key1' [, 'key2']...).
 */
public class SqlAlterModelReset extends SqlAlterModel {
    private final SqlNodeList optionKeyList;

    public SqlAlterModelReset(
            SqlParserPos pos,
            SqlIdentifier modelName,
            boolean ifModelExists,
            SqlNodeList optionKeyList) {
        super(pos, modelName, ifModelExists);
        this.optionKeyList = requireNonNull(optionKeyList, "optionKeyList should not be null");
    }

    @Override
    public List<SqlNode> getOperandList() {
        return List.of(modelName, optionKeyList);
    }

    public Set<String> getResetKeys() {
        return optionKeyList.getList().stream()
                .map(key -> ((NlsString) SqlLiteral.value(key)).getValue())
                .collect(Collectors.toSet());
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("RESET");
        SqlWriter.Frame withFrame = writer.startList("(", ")");
        for (SqlNode optionKey : optionKeyList) {
            SqlUnparseUtils.printIndent(writer);
            optionKey.unparse(writer, leftPrec, rightPrec);
        }
        writer.newlineAndIndent();
        writer.endList(withFrame);
    }
}
