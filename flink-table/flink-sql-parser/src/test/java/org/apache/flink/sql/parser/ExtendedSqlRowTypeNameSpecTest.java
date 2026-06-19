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

import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.type.ExtendedSqlRowTypeNameSpec;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/** Tests for {@link ExtendedSqlRowTypeNameSpec}. */
class ExtendedSqlRowTypeNameSpecTest {
    @Test
    void testExtendedRowWithNoComments() {
        final ExtendedSqlRowTypeNameSpec spec =
                new ExtendedSqlRowTypeNameSpec(
                        SqlParserPos.ZERO,
                        List.of(
                                new SqlIdentifier("t1", SqlParserPos.ZERO),
                                new SqlIdentifier("t2", SqlParserPos.ZERO),
                                new SqlIdentifier("t3", SqlParserPos.ZERO)),
                        List.of(
                                new SqlDataTypeSpec(
                                        new SqlBasicTypeNameSpec(
                                                SqlTypeName.INTEGER, SqlParserPos.ZERO),
                                        SqlParserPos.ZERO),
                                new SqlDataTypeSpec(
                                        new SqlBasicTypeNameSpec(
                                                SqlTypeName.DATE, SqlParserPos.ZERO),
                                        SqlParserPos.ZERO),
                                new SqlDataTypeSpec(
                                        new SqlBasicTypeNameSpec(
                                                SqlTypeName.TIME, SqlParserPos.ZERO),
                                        SqlParserPos.ZERO)),
                        List.of(),
                        false);
        SqlWriter writer = getSqlWriter();
        spec.unparse(writer, 0, 0);
    }

    private SqlWriter getSqlWriter() {
        final Map<String, ?> options =
                Map.ofEntries(
                        Map.entry("quoting", Quoting.BACK_TICK),
                        Map.entry("quotedCasing", Casing.UNCHANGED),
                        Map.entry("unquotedCasing", Casing.UNCHANGED),
                        Map.entry("caseSensitive", true),
                        Map.entry("enableTypeCoercion", false),
                        Map.entry("conformance", SqlConformanceEnum.DEFAULT),
                        Map.entry("operatorTable", SqlStdOperatorTable.instance()),
                        Map.entry("parserFactory", FlinkSqlParserImpl.FACTORY));
        final SqlParser.Config parserConfig =
                SqlParser.config()
                        .withQuoting((Quoting) options.get("quoting"))
                        .withUnquotedCasing((Casing) options.get("unquotedCasing"))
                        .withQuotedCasing((Casing) options.get("quotedCasing"))
                        .withConformance((SqlConformance) options.get("conformance"))
                        .withCaseSensitive((boolean) options.get("caseSensitive"))
                        .withParserFactory((SqlParserImplFactory) options.get("parserFactory"));

        return new SqlPrettyWriter(
                new CalciteSqlDialect(
                        SqlDialect.EMPTY_CONTEXT
                                .withQuotedCasing(parserConfig.unquotedCasing())
                                .withConformance(parserConfig.conformance())
                                .withUnquotedCasing(parserConfig.unquotedCasing())
                                .withIdentifierQuoteString(parserConfig.quoting().string)),
                false);
    }
}
