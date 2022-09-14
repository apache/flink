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

package org.apache.flink.sql.parser.expr;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;

/** This is the unresolved version of {@code TRY_CAST}. We need it only for unparse. */
public class SqlUnresolvedTryCastFunction extends SqlUnresolvedFunction {

    public SqlUnresolvedTryCastFunction(SqlParserPos pos) {
        super(
                new SqlIdentifier(
                        Collections.singletonList("TRY_CAST"),
                        null,
                        pos,
                        Collections.singletonList(pos)),
                null,
                null,
                null,
                null,
                SqlFunctionCategory.SYSTEM);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        assert call.operandCount() == 2;
        final SqlWriter.Frame frame = writer.startFunCall(getName());
        call.operand(0).unparse(writer, 0, 0);
        writer.sep("AS");
        if (call.operand(1) instanceof SqlIntervalQualifier) {
            writer.sep("INTERVAL");
        }
        call.operand(1).unparse(writer, 0, 0);
        writer.endFunCall(frame);
    }
}
