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

package org.apache.flink.sql.parser;

import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.impl.ParseException;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.util.SourceStringReader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.of;

/** Tests for parsing a Table API specific SqlIdentifier. */
class TableApiIdentifierParsingTest {

    private static final String ANTHROPOS_IN_GREEK_IN_UNICODE =
            "#03B1#03BD#03B8#03C1#03C9#03C0#03BF#03C2";
    private static final String ANTHROPOS_IN_GREEK = "ανθρωπος";

    static Stream<Arguments> parameters() {
        return Stream.of(
                of("array", singletonList("array")),
                of("table", singletonList("table")),
                of("cat.db.array", asList("cat", "db", "array")),
                of("`cat.db`.table", asList("cat.db", "table")),
                of("db.table", asList("db", "table")),
                of("`ta``ble`", singletonList("ta`ble")),
                of("`c``at`.`d``b`.`ta``ble`", asList("c`at", "d`b", "ta`ble")),
                of(
                        "db.U&\"" + ANTHROPOS_IN_GREEK_IN_UNICODE + "\" UESCAPE '#'",
                        asList("db", ANTHROPOS_IN_GREEK)),
                of("db.ανθρωπος", asList("db", ANTHROPOS_IN_GREEK)));
    }

    @ParameterizedTest(name = "Parsing: {0}. Expected identifier: {1}")
    @MethodSource("parameters")
    void testTableApiIdentifierParsing(
            String stringIdentifier, List<String> expectedParsedIdentifier) throws ParseException {
        FlinkSqlParserImpl parser = createFlinkParser(stringIdentifier);

        SqlIdentifier sqlIdentifier = parser.TableApiIdentifier();
        assertThat(sqlIdentifier.names).isEqualTo(expectedParsedIdentifier);
    }

    private FlinkSqlParserImpl createFlinkParser(String expr) {
        SourceStringReader reader = new SourceStringReader(expr);
        FlinkSqlParserImpl parser =
                (FlinkSqlParserImpl) FlinkSqlParserImpl.FACTORY.getParser(reader);
        parser.setTabSize(1);
        parser.setUnquotedCasing(Lex.JAVA.unquotedCasing);
        parser.setQuotedCasing(Lex.JAVA.quotedCasing);
        parser.setIdentifierMaxLength(256);
        parser.setConformance(FlinkSqlConformance.DEFAULT);
        parser.switchTo(SqlAbstractParserImpl.LexicalState.BTID);

        return parser;
    }
}
