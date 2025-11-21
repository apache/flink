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

import org.apache.flink.sql.parser.ddl.SqlDistribution;
import org.apache.flink.sql.parser.ddl.SqlRefreshMode;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;

import javax.annotation.Nullable;

import java.util.List;

/** Utils to unparse DDLs. */
public class SqlUnparseUtils {

    private SqlUnparseUtils() {}

    public static void unparseTableSchema(
            SqlNodeList columnList,
            List<SqlTableConstraint> constraints,
            @Nullable SqlWatermark watermark,
            SqlWriter writer,
            int leftPrec,
            int rightPrec) {
        if (columnList.isEmpty() && constraints.isEmpty() && watermark == null) {
            return;
        }

        final SqlWriter.Frame frame =
                writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
        for (SqlNode column : columnList) {
            printIndent(writer);
            column.unparse(writer, leftPrec, rightPrec);
        }

        if (!constraints.isEmpty()) {
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

    public static void unparseDistribution(
            SqlDistribution distribution, SqlWriter writer, int leftPrec, int rightPrec) {
        if (distribution == null) {
            return;
        }
        writer.newlineAndIndent();
        distribution.unparse(writer, leftPrec, rightPrec);
    }

    public static void unparsePartitionKeyList(
            SqlNodeList partitionKeyList, SqlWriter writer, int leftPrec, int rightPrec) {
        if (partitionKeyList.isEmpty()) {
            return;
        }
        writer.newlineAndIndent();
        writer.keyword("PARTITIONED BY");
        SqlWriter.Frame partitionedByFrame = writer.startList("(", ")");
        partitionKeyList.unparse(writer, leftPrec, rightPrec);
        writer.endList(partitionedByFrame);
    }

    public static void unparseFreshness(
            SqlIntervalLiteral freshness,
            boolean withNewLine,
            SqlWriter writer,
            int leftPrec,
            int rightPrec) {
        if (freshness == null) {
            return;
        }
        if (withNewLine) {
            writer.newlineAndIndent();
        }
        writer.keyword("FRESHNESS");
        writer.keyword("=");
        freshness.unparse(writer, leftPrec, rightPrec);
    }

    public static void unparseRefreshMode(SqlRefreshMode refreshMode, SqlWriter writer) {
        if (refreshMode == null) {
            return;
        }
        writer.newlineAndIndent();
        writer.keyword("REFRESH_MODE");
        writer.keyword("=");
        writer.keyword(refreshMode.name());
    }

    public static void unparseComment(
            SqlCharStringLiteral comment,
            boolean withNewLine,
            SqlWriter writer,
            int leftPrec,
            int rightPrec) {
        if (comment == null) {
            return;
        }
        if (withNewLine) {
            writer.newlineAndIndent();
        }
        writer.keyword("COMMENT");
        comment.unparse(writer, leftPrec, rightPrec);
    }

    public static void unparseSetOptions(
            SqlNodeList propertyList, SqlWriter writer, int leftPrec, int rightPrec) {
        unparseListWithIndent("SET", propertyList, false, writer, leftPrec, rightPrec);
    }

    public static void unparseResetOptions(
            SqlNodeList propertyList, SqlWriter writer, int leftPrec, int rightPrec) {
        unparseListWithIndent("RESET", propertyList, false, writer, leftPrec, rightPrec);
    }

    public static void unparseAsQuery(
            SqlNode query, SqlWriter writer, int leftPrec, int rightPrec) {
        writer.newlineAndIndent();
        writer.keyword("AS");
        writer.newlineAndIndent();
        query.unparse(writer, leftPrec, rightPrec);
    }

    public static void unparseLanguage(String functionLanguage, SqlWriter writer) {
        if (functionLanguage != null) {
            writer.keyword("LANGUAGE");
            writer.keyword(functionLanguage);
        }
    }

    public static void unparseProperties(
            SqlNodeList properties, SqlWriter writer, int leftPrec, int rightPrec) {
        if (properties == null || properties.isEmpty()) {
            return;
        }
        writer.newlineAndIndent();
        unparseListWithIndent("WITH", properties, true, writer, leftPrec, rightPrec);
    }

    public static void unparseList(
            SqlNodeList list, SqlWriter writer, int leftPrec, int rightPrec) {
        SqlWriter.Frame withFrame = writer.startList("(", ")");
        list.unparse(writer, leftPrec, rightPrec);
        writer.endList(withFrame);
    }

    public static void unparseListWithIndent(
            String name,
            SqlNodeList list,
            boolean skipIfEmpty,
            SqlWriter writer,
            int leftPrec,
            int rightPrec) {
        if (skipIfEmpty && list.isEmpty()) {
            return;
        }
        writer.keyword(name);
        SqlWriter.Frame withFrame = writer.startList("(", ")");
        for (SqlNode property : list) {
            SqlUnparseUtils.printIndent(writer);
            property.unparse(writer, leftPrec, rightPrec);
        }
        writer.newlineAndIndent();
        writer.endList(withFrame);
    }
}
