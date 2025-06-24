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

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.client.cli.parser.Command.CLEAR;
import static org.apache.flink.table.client.cli.parser.Command.HELP;
import static org.apache.flink.table.client.cli.parser.Command.OTHER;
import static org.apache.flink.table.client.cli.parser.Command.QUIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Testing whether {@link SqlCommandParserImpl} can parse statement to get {@link Command}
 * correctly.
 */
public class SqlCommandParserImplTest {

    private static final String EXECUTE_STATEMENT_SET =
            "EXECUTE STATEMENT SET BEGIN\n INSERT INTO StreamingTable SELECT * FROM (VALUES (1, 'Hello World'));";

    private final SqlCommandParserImpl sqlCommandParserImpl = new SqlCommandParserImpl();

    @ParameterizedTest
    @MethodSource("positiveCases")
    public void testParseStatement(TestSpec testData) {
        Command command = sqlCommandParserImpl.parseStatement(testData.statement).orElse(null);
        assertThat(command).isEqualTo(testData.command);
    }

    @ParameterizedTest
    @MethodSource("negativeCases")
    public void testParseIncompleteStatement(String statement) {
        assertThatThrownBy(() -> sqlCommandParserImpl.parseStatement(statement))
                .satisfies(anyCauseMatches(SqlExecutionException.class))
                .cause()
                .satisfies(anyCauseMatches(SqlParserEOFException.class));
    }

    private static List<TestSpec> positiveCases() {
        return Arrays.asList(
                TestSpec.of(";", null),
                TestSpec.of("; ;", OTHER),
                // comment and multi lines tests
                TestSpec.of("SHOW --ignore;\n CREATE TABLE tbl;", OTHER),
                TestSpec.of("SHOW\n create\t TABLE `tbl`;", OTHER),
                TestSpec.of("SHOW -- create\n TABLES;", OTHER),
                // special characters tests
                TestSpec.of("SELECT * FROM `tbl`;", OTHER),
                TestSpec.of("SHOW /* ignore */ CREATE TABLE \"tbl\";", OTHER),
                TestSpec.of("SELECT '\\';", OTHER),
                // normal tests
                TestSpec.of("quit;", QUIT), // non case sensitive test
                TestSpec.of("QUIT;", QUIT),
                TestSpec.of("Quit;", QUIT),
                TestSpec.of("QuIt;", QUIT),
                TestSpec.of("clear;", CLEAR),
                TestSpec.of("help;", HELP),
                TestSpec.of("EXPLAIN PLAN FOR 'what_ever';", OTHER),
                TestSpec.of("SHOW CREATE TABLE(what_ever);", OTHER),
                TestSpec.of("SHOW CREATE VIEW (what_ever);", OTHER),
                TestSpec.of("SHOW CREATE syntax_error;", OTHER),
                TestSpec.of("SHOW TABLES;", OTHER),
                TestSpec.of("BEGIN STATEMENT SET;", OTHER),
                TestSpec.of("BEGIN statement;", OTHER),
                TestSpec.of("END;", OTHER),
                TestSpec.of("END statement;", OTHER),
                // statement set tests
                TestSpec.of(EXECUTE_STATEMENT_SET, OTHER),
                TestSpec.of("EXPLAIN " + EXECUTE_STATEMENT_SET, OTHER),
                TestSpec.of("EXPLAIN BEGIN STATEMENT SET;", OTHER),
                TestSpec.of(EXECUTE_STATEMENT_SET + "\nEND;", OTHER),
                TestSpec.of("EXPLAIN " + EXECUTE_STATEMENT_SET + "\nEND;", OTHER),
                TestSpec.of("EXPLAIN BEGIN STATEMENT SET;\nEND;", OTHER));
    }

    private static List<String> negativeCases() {
        return Arrays.asList("-- comment;", "SHOW TABLES -- comment;", "SHOW TABLES");
    }

    /** Used to load generated data. */
    private static class TestSpec {
        String statement;
        @Nullable Command command;

        TestSpec(String statement, @Nullable Command command) {
            this.statement = statement;
            this.command = command;
        }

        @Override
        public String toString() {
            return "TestSpec{" + "statement='" + statement + '\'' + ", command=" + command + '}';
        }

        static TestSpec of(String statement, @Nullable Command command) {
            return new TestSpec(statement, command);
        }
    }
}
