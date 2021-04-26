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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

/** Test for {@link ParserImpl}. */
public class ParserImplTest {

    @Rule public ExpectedException thrown = ExpectedException.none();

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
                    t ->
                            plannerContext.createSqlExprToRexConverter(
                                    plannerContext.getTypeFactory().buildRelNodeRowType(t)));

    private List<TestSpec> testLegalStatements;
    private List<TestSpec> testIllegalStatements;

    @Before
    public void setup() {
        testLegalStatements =
                Arrays.asList(
                        TestSpec.forStatement("ClEaR").expectedSummary("CLEAR"),
                        TestSpec.forStatement("hElP").expectedSummary("HELP"),
                        TestSpec.forStatement("qUIT").expectedSummary("QUIT"),
                        TestSpec.forStatement("ExIT").expectedSummary("EXIT"),
                        TestSpec.forStatement("REsEt").expectedSummary("RESET"),
                        TestSpec.forStatement("   SEt ").expectedSummary("SET"),
                        TestSpec.forStatement("SET execution.runtime-type=batch")
                                .expectedSummary("SET execution.runtime-type=batch"),
                        TestSpec.forStatement("SET pipeline.jars = /path/to/test-_-jar.jar")
                                .expectedSummary("SET pipeline.jars=/path/to/test-_-jar.jar"),
                        TestSpec.forStatement("USE test.db1").expectedSummary("USE test.db1"),
                        TestSpec.forStatement("SHOW tables").expectedSummary("SHOW TABLES"),
                        TestSpec.forStatement("SET pipeline.name = ' '")
                                .expectedSummary("SET pipeline.name = ' '"));

        testIllegalStatements =
                Collections.singletonList(TestSpec.forStatement("SET execution.runtime-type="));
    }

    @Test
    public void testParseLegalStatements() {
        for (TestSpec spec : testLegalStatements) {
            Operation op = parser.parse(spec.statement).get(0);
            Assert.assertEquals(op.asSummaryString(), op.asSummaryString());
        }
    }

    @Test
    public void testParseIllegalStatements() {
        thrown.expect(SqlParserException.class);
        for (TestSpec spec : testIllegalStatements) {
            parser.parse(spec.statement);
            fail("Should fail.");
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

    private static class TestSpec {

        private String statement;
        private String expectedSummary;

        private TestSpec(String statement) {
            this.statement = statement;
        }

        static TestSpec forStatement(String statement) {
            return new TestSpec(statement);
        }

        TestSpec expectedSummary(String expectedSummary) {
            this.expectedSummary = expectedSummary;
            return this;
        }
    }

    private void verifySqlCompletion(String statement, int position, String[] expectedHints) {
        String[] hints = parser.getCompletionHints(statement, position);
        assertArrayEquals(expectedHints, hints);
    }
}
