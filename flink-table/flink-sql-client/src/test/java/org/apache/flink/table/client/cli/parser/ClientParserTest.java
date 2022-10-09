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

import org.apache.flink.table.api.SqlParserEOFException;
import org.apache.flink.table.client.gateway.SqlExecutionException;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.client.cli.parser.StatementType.BEGIN_STATEMENT_SET;
import static org.apache.flink.table.client.cli.parser.StatementType.CLEAR;
import static org.apache.flink.table.client.cli.parser.StatementType.END;
import static org.apache.flink.table.client.cli.parser.StatementType.EXPLAIN;
import static org.apache.flink.table.client.cli.parser.StatementType.HELP;
import static org.apache.flink.table.client.cli.parser.StatementType.OTHER;
import static org.apache.flink.table.client.cli.parser.StatementType.QUIT;
import static org.apache.flink.table.client.cli.parser.StatementType.SELECT;
import static org.apache.flink.table.client.cli.parser.StatementType.SHOW_CREATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Testing whether {@link ClientParser} can parse statement to get {@link StatementType} correctly.
 */
public class ClientParserTest {

    private static final String EXECUTE_STATEMENT_SET =
            "EXECUTE STATEMENT SET BEGIN\n INSERT INTO StreamingTable SELECT * FROM (VALUES (1, 'Hello World'));";

    private final ClientParser clientParser = new ClientParser();

    @ParameterizedTest
    @MethodSource("positiveCases")
    public void testParseStatement(TestSpec testData) {
        Optional<StatementType> type = clientParser.parseStatement(testData.statement);
        assertThat(type.orElse(null)).isEqualTo(testData.type);
    }

    @ParameterizedTest
    @MethodSource("negativeCases")
    public void testParseIncompleteStatement(String statement) {
        assertThatThrownBy(() -> clientParser.parseStatement(statement))
                .satisfies(anyCauseMatches(SqlExecutionException.class))
                .cause()
                .satisfies(anyCauseMatches(SqlParserEOFException.class));
    }

    private static List<TestSpec> positiveCases() {
        return Arrays.asList(
                TestSpec.of(";", OTHER),
                TestSpec.of("; ;", OTHER),
                // comment and multi lines tests
                TestSpec.of("SHOW --ignore;\n CREATE TABLE tbl;", SHOW_CREATE),
                TestSpec.of("SHOW\n create\t TABLE `tbl`;", SHOW_CREATE),
                TestSpec.of("SHOW -- create\n TABLES;", OTHER),
                // special characters tests
                TestSpec.of("SELECT * FROM `tbl`;", SELECT),
                TestSpec.of("SHOW /* ignore */ CREATE TABLE \"tbl\";", SHOW_CREATE),
                TestSpec.of("SELECT '\\';", SELECT),
                // normal tests
                TestSpec.of("quit;", QUIT), // non case sensitive test
                TestSpec.of("QUIT;", QUIT),
                TestSpec.of("Quit;", QUIT),
                TestSpec.of("QuIt;", QUIT),
                TestSpec.of("clear;", CLEAR),
                TestSpec.of("help;", HELP),
                TestSpec.of("EXPLAIN PLAN FOR 'what_ever';", EXPLAIN),
                TestSpec.of("SHOW CREATE TABLE(what_ever);", SHOW_CREATE),
                TestSpec.of("SHOW CREATE VIEW (what_ever);", SHOW_CREATE),
                TestSpec.of("SHOW CREATE syntax_error;", SHOW_CREATE),
                TestSpec.of("SHOW TABLES;", OTHER),
                TestSpec.of("BEGIN STATEMENT SET;", BEGIN_STATEMENT_SET),
                TestSpec.of("BEGIN statement;", OTHER),
                TestSpec.of("END;", END),
                TestSpec.of("END statement;", OTHER),
                // statement set tests
                TestSpec.of(EXECUTE_STATEMENT_SET, OTHER),
                TestSpec.of("EXPLAIN " + EXECUTE_STATEMENT_SET, EXPLAIN),
                TestSpec.of("EXPLAIN BEGIN STATEMENT SET;", EXPLAIN),
                TestSpec.of(EXECUTE_STATEMENT_SET + "\nEND;", OTHER),
                TestSpec.of("EXPLAIN " + EXECUTE_STATEMENT_SET + "\nEND;", EXPLAIN),
                TestSpec.of("EXPLAIN BEGIN STATEMENT SET;\nEND;", EXPLAIN));
    }

    private static List<String> negativeCases() {
        return Arrays.asList(
                "", "\n", " ", "-- comment;", "SHOW TABLES -- comment;", "SHOW TABLES");
    }

    /** Used to load generated data. */
    private static class TestSpec {
        String statement;
        StatementType type;

        TestSpec(String statement, @Nullable StatementType type) {
            this.statement = statement;
            this.type = type;
        }

        @Override
        public String toString() {
            return "TestSpec{" + "statement='" + statement + '\'' + ", type=" + type + '}';
        }

        static TestSpec of(String statement, @Nullable StatementType type) {
            return new TestSpec(statement, type);
        }
    }
}
