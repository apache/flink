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

package org.apache.flink.sql.parser.hive.type;

import org.apache.flink.sql.parser.hive.impl.ParseException;
import org.apache.flink.sql.parser.type.ExtendedSqlRowTypeNameSpec;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

import java.util.List;

/** To represent STRUCT type in Hive. */
public class ExtendedHiveStructTypeNameSpec extends ExtendedSqlRowTypeNameSpec {

    public ExtendedHiveStructTypeNameSpec(
            SqlParserPos pos,
            List<SqlIdentifier> fieldNames,
            List<SqlDataTypeSpec> fieldTypes,
            List<SqlCharStringLiteral> comments)
            throws ParseException {
        super(pos, fieldNames, fieldTypes, comments, false);
        if (fieldNames.isEmpty()) {
            throw new ParseException("STRUCT with no fields is not allowed");
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print("STRUCT");
        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "<", ">");
        int i = 0;
        for (Pair<SqlIdentifier, SqlDataTypeSpec> p : Pair.zip(getFieldNames(), getFieldTypes())) {
            writer.sep(",", false);
            p.left.unparse(writer, 0, 0);
            p.right.unparse(writer, leftPrec, rightPrec);
            if (p.right.getNullable() != null && !p.right.getNullable()) {
                writer.keyword("NOT NULL");
            }
            if (getComments().get(i) != null) {
                getComments().get(i).unparse(writer, leftPrec, rightPrec);
            }
            i += 1;
        }
        writer.endList(frame);
    }
}
