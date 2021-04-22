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

import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.junit.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;
import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.apache.flink.table.planner.delegation.ParserImplTest.TestSpec.forStatement;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Test for {@link ParserImpl}. */
public class ParserImplTest {

    private final boolean isStreamingMode = false;
    private final TableConfig tableConfig = new TableConfig();
    private final Catalog catalog = new GenericInMemoryCatalog("MockCatalog", "default");
    private final CatalogManager catalogManager =
            CatalogManagerMocks.preparedCatalogManager().defaultCatalog("builtin", catalog).build();
    private final ModuleManager moduleManager = new ModuleManager();
    private final FunctionCatalog functionCatalog =
            new FunctionCatalog(tableConfig, catalogManager, moduleManager);
    private final PlannerContext plannerContext =
            new PlannerContext(
                    tableConfig,
                    functionCatalog,
                    catalogManager,
                    asRootSchema(new CatalogManagerCalciteSchema(catalogManager, isStreamingMode)),
                    new ArrayList<>());

    private final Supplier<FlinkPlannerImpl> plannerSupplier =
            () ->
                    plannerContext.createFlinkPlanner(
                            catalogManager.getCurrentCatalog(),
                            catalogManager.getCurrentDatabase());

    private final Parser parser =
            new ParserImpl(
                    catalogManager,
                    plannerSupplier,
                    () -> plannerSupplier.get().parser(),
                    plannerContext.getSqlExprToRexConverterFactory());

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
                            .error("SQL parse failed. Encountered \"-\" at line 1, column 22"));

    @Test
    public void testParseLegalStatements() {
        for (TestSpec spec : TEST_SPECS) {
            if (spec.expectedSummary != null) {
                Operation op = parser.parse(spec.statement).get(0);
                assertEquals(spec.expectedSummary, op.asSummaryString());
            }

            if (spec.expectedError != null) {
                assertThrows(
                        spec.expectedError,
                        SqlParserException.class,
                        () -> parser.parse(spec.statement));
            }
        }
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
        assertArrayEquals(expectedHints, hints);
    }
}
