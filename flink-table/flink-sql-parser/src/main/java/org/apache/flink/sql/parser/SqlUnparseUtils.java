/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.sql.parser;

import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;

import javax.annotation.Nullable;

import java.util.List;

/** Utils to unparse DDLs. */
public class SqlUnparseUtils {

    private SqlUnparseUtils() {}

    public static void unparseTableSchema(
            SqlWriter writer,
            int leftPrec,
            int rightPrec,
            SqlNodeList columnList,
            List<SqlTableConstraint> constraints,
            @Nullable SqlWatermark watermark) {
        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
        for (SqlNode column : columnList) {
            printIndent(writer);
            column.unparse(writer, leftPrec, rightPrec);
        }
        if (constraints.size() > 0) {
            for (SqlTableConstraint constraint : constraints) {
                printIndent(writer);
                constraint.unparse(writer, leftPrec, rightPrec);
            }
        }
        if (watermark != null) {
            printIndent(writer);
            watermark.unparse(writer, leftPrec, rightPrec);
        }

        writer.newlineAndIndent();
        writer.endList(frame);
    }

    public static void printIndent(SqlWriter writer) {
        writer.sep(",", false);
        writer.newlineAndIndent();
        writer.print("  ");
    }
}
