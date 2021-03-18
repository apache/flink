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
import org.apache.flink.table.operations.command.ClearOperation;
import org.apache.flink.table.operations.command.HelpOperation;
import org.apache.flink.table.operations.command.QuitOperation;
import org.apache.flink.table.operations.command.ResetOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.table.operations.command.SourceOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

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

    @Test
    public void testClearCommand() {
        assertSimpleCommand("ClEaR", instanceOf(ClearOperation.class));
    }

    @Test
    public void testHelpCommand() {
        assertSimpleCommand("hELp", instanceOf(HelpOperation.class));
    }

    @Test
    public void testQuitCommand() {
        assertSimpleCommand("qUIt", instanceOf(QuitOperation.class));
        assertSimpleCommand("Exit", instanceOf(QuitOperation.class));
    }

    @Test
    public void testResetCommand() {
        assertSimpleCommand("REsEt", instanceOf(ResetOperation.class));
    }

    @Test
    public void testSetOperation() {
        assertSetCommand("   SEt       ");
        assertSetCommand("SET execution.runtime-type= batch", "execution.runtime-type", "batch");
        assertSetCommand(
                "SET pipeline.jars = /path/to/test-_-jar.jar",
                "pipeline.jars",
                "/path/to/test-_-jar.jar");

        assertFailedSetCommand("SET execution.runtime-type=");
    }

    @Test
    public void testGetCompletionHints() {
        String[] hints = parser.getCompletionHints("SE", 2);
        System.out.println(hints[0]);
    }

    @Test
    public void testSourceOperation() {
        List<String> paths =
                Arrays.asList("/path/to/file", "oss://path/hello.go", "test\\ jar.jar");

        for (String path : paths) {
            SourceOperation sourceOperation =
                    (SourceOperation) parser.parse(String.format("SoUrce   %s", path)).get(0);
            assertEquals(path, sourceOperation.getPath());
        }
    }

    @Test
    public void testCompletionTest() {
        verifySqlCompletion("QU", 1, new String[] {"QUIT"});
        verifySqlCompletion("SE", 1, new String[] {"SET"});
        verifySqlCompletion("Sou", 1, new String[] {"SOURCE"});
        verifySqlCompletion(
                "", 0, new String[] {"CLEAR", "HELP", "EXIT", "QUIT", "RESET", "SET", "SOURCE"});

        verifySqlCompletion("SELECT a fram b", 10, new String[] {"FETCH", "FROM"});
    }

    // ~ Tool Methods ----------------------------------------------------------

    private void assertSimpleCommand(String statement, Matcher<? super Operation> matcher) {
        Operation operation = parser.parse(statement).get(0);
        assertThat(operation, matcher);
    }

    private void assertSetCommand(String statement, String... operands) {
        SetOperation operation = (SetOperation) parser.parse(statement).get(0);

        assertArrayEquals(operands, operation.getOperands());
    }

    private void assertFailedSetCommand(String statement) {
        thrown.expect(SqlParserException.class);

        parser.parse(statement);
    }

    private void verifySqlCompletion(String statement, int position, String[] expectedHints) {
        String[] hints = parser.getCompletionHints(statement, position);
        assertArrayEquals(expectedHints, hints);
    }
}
