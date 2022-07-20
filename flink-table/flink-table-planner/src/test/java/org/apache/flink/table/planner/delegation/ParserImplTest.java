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

package org.apache.flink.table.planner.delegation;

import org.apache.flink.table.api.SqlParserEOFException;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.utils.PlannerMocks;

import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.flink.table.planner.delegation.ParserImplTest.TestSpec.forStatement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ParserImpl}. */
public class ParserImplTest {

    private final PlannerMocks plannerMocks = PlannerMocks.create(true);

    private final Supplier<FlinkPlannerImpl> plannerSupplier = plannerMocks::getPlanner;

    private final Parser parser =
            new ParserImpl(
                    plannerMocks.getCatalogManager(),
                    plannerSupplier,
                    () -> plannerSupplier.get().parser(),
                    plannerMocks.getPlannerContext().getRexFactory());

    private static final List<TestSpec> TEST_SPECS =
            Arrays.asList(
                    forStatement("ClEaR").summary("CLEAR"),
                    forStatement("hElP").summary("HELP"),
                    forStatement("qUIT").summary("QUIT"),
                    forStatement("ExIT").summary("QUIT"),
                    forStatement("REsEt").summary("RESET"),
                    forStatement("REsEt execution.runtime-type")
                            .summary("RESET execution.runtime-type"),
                    forStatement("   SEt ").summary("SET"),
                    forStatement("SET execution.runtime-type=batch")
                            .summary("SET execution.runtime-type=batch"),
                    forStatement("SET pipeline.jars = /path/to/test-_-jar.jar")
                            .summary("SET pipeline.jars=/path/to/test-_-jar.jar"),
                    forStatement("USE test.db1").summary("USE test.db1"),
                    forStatement("SHOW tables").summary("SHOW TABLES"),
                    forStatement("SET pipeline.name = 'test name'")
                            .summary("SET pipeline.name=test name"),
                    forStatement("SET pipeline.name = ' '").summary("SET pipeline.name= "),
                    forStatement("SET execution.runtime-type=")
                            // TODO: the exception message should be "no value defined"
                            .error(
                                    "SQL parse failed. Encountered \"execution\" at line 1, column 5"));

    @Test
    public void testParseLegalStatements() {
        for (TestSpec spec : TEST_SPECS) {
            if (spec.expectedSummary != null) {
                Operation op = parser.parse(spec.statement).get(0);
                assertThat(op.asSummaryString()).isEqualTo(spec.expectedSummary);
            }

            if (spec.expectedError != null) {
                assertThatThrownBy(() -> parser.parse(spec.statement))
                        .isInstanceOf(SqlParserException.class)
                        .hasMessageContaining(spec.expectedError);
            }
        }
    }

    @Test
    public void testPartialParse() {
        assertThatThrownBy(() -> parser.parse("select A From"))
                .isInstanceOf(SqlParserEOFException.class);
    }

    @Test
    public void testPartialParseWithStatementSet() {
        assertThatThrownBy(
                        () ->
                                parser.parse(
                                        "Execute Statement Set Begin insert into A select * from B"))
                .isInstanceOf(SqlParserEOFException.class);
    }

    @Test
    public void testCompletionTest() {
        verifySqlCompletion("QU", 1, new String[] {"QUIT"});
        verifySqlCompletion("SE", 1, new String[] {"SET"});
        verifySqlCompletion("", 0, new String[] {"CLEAR", "HELP", "EXIT", "QUIT", "RESET", "SET"});
        verifySqlCompletion("SELECT a fram b", 10, new String[] {"FETCH", "FROM"});
    }

    // ~ Tool Methods ----------------------------------------------------------

    static class TestSpec {

        private final String statement;

        @Nullable private String expectedSummary;

        @Nullable private String expectedError;

        private TestSpec(String statement) {
            this.statement = statement;
        }

        static TestSpec forStatement(String statement) {
            return new TestSpec(statement);
        }

        TestSpec summary(String expectedSummary) {
            this.expectedSummary = expectedSummary;
            return this;
        }

        TestSpec error(String expectedError) {
            this.expectedError = expectedError;
            return this;
        }
    }

    private void verifySqlCompletion(String statement, int position, String[] expectedHints) {
        String[] hints = parser.getCompletionHints(statement, position);
        assertThat(hints).isEqualTo(expectedHints);
    }
}
