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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

/**
 * Properties of sql pattern. Expanding new operators on the basis of {@link SqlStdOperatorTable}
 */
public class SqlOperatorPattern extends ReflectiveSqlOperatorTable {

    public static final SqlSpecialOperator PATTERN_NEGATIVE =
            new SqlSpecialOperator("PATTERN_NEGATIVE", SqlKind.OTHER, 100) {
                @Override
                public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
                    SqlWriter.Frame frame = writer.startList("[^", "]");
                    SqlNode node = call.getOperandList().get(0);
                    node.unparse(writer, 0, 0);
                    writer.endList(frame);
                }
            };
}
