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

package org.apache.flink.table.client.cli.parser;

import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.Ignore;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Testing whether {@link ClientParser} can parse statement to get {@link StatementType} correctly.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class ClientParserTest {

    private final ClientParser clientParser = new ClientParser();

    private static final Optional<StatementType> QUIT = Optional.of(StatementType.QUIT);
    private static final Optional<StatementType> CLEAR = Optional.of(StatementType.CLEAR);
    private static final Optional<StatementType> HELP = Optional.of(StatementType.HELP);
    private static final Optional<StatementType> EXPLAIN = Optional.of(StatementType.EXPLAIN);
    private static final Optional<StatementType> SHOW_CREATE =
            Optional.of(StatementType.SHOW_CREATE);
    private static final Optional<StatementType> OTHER = Optional.of(StatementType.OTHER);
    private static final Optional<StatementType> EMPTY = Optional.empty();

    @Ignore
    @ParameterizedTest
    @MethodSource("generateTestData")
    public void testParseStatement(Tuple2<String, Optional<StatementType>> testData) {
        Optional<StatementType> type = clientParser.parseStatement(testData.f0);
        assertThat(type).isEqualTo(testData.f1);
    }

    private static List<Tuple2<String, Optional<StatementType>>> generateTestData() {
        return Arrays.asList(
                Tuple2.of("quit;", QUIT),
                Tuple2.of("quit", QUIT),
                Tuple2.of("QUIT", QUIT),
                Tuple2.of("Quit", QUIT),
                Tuple2.of("QuIt", QUIT),
                Tuple2.of("clear;", CLEAR),
                Tuple2.of("help;", HELP),
                Tuple2.of("EXPLAIN PLAN FOR what_ever", EXPLAIN),
                Tuple2.of("SHOW CREATE TABLE(what_ever);", SHOW_CREATE),
                Tuple2.of("SHOW CREATE VIEW (what_ever)", SHOW_CREATE),
                Tuple2.of("SHOW CREATE syntax_error;", OTHER),
                Tuple2.of("--SHOW CREATE TABLE ignore_comment", EMPTY),
                Tuple2.of("SHOW CREATE --TABLE ignore_comment", OTHER),
                Tuple2.of("SHOW -- CREATE VIEW ignore_comment", OTHER),
                Tuple2.of("SHOW TABLES;", OTHER),
                Tuple2.of("\tSHOW    CREATE \tTABLE \t ", SHOW_CREATE),
                Tuple2.of("", EMPTY),
                Tuple2.of(";", EMPTY),
                Tuple2.of("\t", EMPTY),
                Tuple2.of("\n", EMPTY));
    }
}
