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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * ALTER MODEL [IF EXISTS] [[catalogName.] dataBasesName.]modelName SET ( name=value [,
 * name=value]*).
 */
public class SqlAlterModelSet extends SqlAlterModel {

    private final SqlNodeList modelOptionList;

    public SqlAlterModelSet(
            SqlParserPos pos,
            SqlIdentifier modelName,
            boolean ifModelExists,
            SqlNodeList modelOptionList) {
        super(pos, modelName, ifModelExists);
        this.modelOptionList =
                requireNonNull(modelOptionList, "modelOptionList should not be null");
    }

    public SqlNodeList getOptionList() {
        return modelOptionList;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return List.of(modelName, modelOptionList);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("SET");
        SqlWriter.Frame withFrame = writer.startList("(", ")");
        for (SqlNode modelOption : modelOptionList) {
            SqlUnparseUtils.printIndent(writer);
            modelOption.unparse(writer, leftPrec, rightPrec);
        }
        writer.newlineAndIndent();
        writer.endList(withFrame);
    }
}
