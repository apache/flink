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

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.table.utils.CatalogManagerMocks.DEFAULT_CATALOG;
import static org.apache.flink.table.utils.CatalogManagerMocks.DEFAULT_DATABASE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

/** Tests for {@link FunctionCatalog}. */
public class FunctionCatalogTest {

    private static final ScalarFunction FUNCTION_1 = new TestFunction1();

    private static final ScalarFunction FUNCTION_2 = new TestFunction2();

    private static final ScalarFunction FUNCTION_3 = new TestFunction3();

    private static final ScalarFunction FUNCTION_4 = new TestFunction4();

    private static final ScalarFunction FUNCTION_INVALID = new InvalidTestFunction();

    private static final TableFunction<?> TABLE_FUNCTION = new TestTableFunction();

    private static final AggregateFunction<?, ?> AGGREGATE_FUNCTION = new TestAggregateFunction();

    private static final String NAME = "test_function";

    private static final ObjectIdentifier IDENTIFIER =
            ObjectIdentifier.of(DEFAULT_CATALOG, DEFAULT_DATABASE, NAME);

    private static final UnresolvedIdentifier FULL_UNRESOLVED_IDENTIFIER =
            UnresolvedIdentifier.of(DEFAULT_CATALOG, DEFAULT_DATABASE, NAME);

    private static final UnresolvedIdentifier PARTIAL_UNRESOLVED_IDENTIFIER =
            UnresolvedIdentifier.of(NAME);

    private ModuleManager moduleManager;

    private FunctionCatalog functionCatalog;

    private Catalog catalog;

    @Before
    public void init() {
        catalog = new GenericInMemoryCatalog(DEFAULT_CATALOG, DEFAULT_DATABASE);

        moduleManager = new ModuleManager();

        functionCatalog =
                new FunctionCatalog(
                        TableConfig.getDefault(),
                        CatalogManagerMocks.preparedCatalogManager()
                                .defaultCatalog(DEFAULT_CATALOG, catalog)
                                .build(),
                        moduleManager);
    }

    @Test
    public void testGetBuiltInFunctions() {
        Set<String> actual = new HashSet<>();
        Collections.addAll(actual, functionCatalog.getFunctions());

        Set<String> expected = new ModuleManager().listFunctions();

        assertTrue(actual.containsAll(expected));
    }

    @Test
    public void testPreciseFunctionReference() throws Exception {
        // test no function is found
        assertFalse(functionCatalog.lookupFunction(FULL_UNRESOLVED_IDENTIFIER).isPresent());

        // test catalog function is found
        catalog.createFunction(
                IDENTIFIER.toObjectPath(),
                new CatalogFunctionImpl(FUNCTION_1.getClass().getName()),
                false);

        assertThat(
                functionCatalog.lookupFunction(FULL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(IDENTIFIER), FUNCTION_1));

        // test temp catalog function is found
        functionCatalog.registerTemporaryCatalogFunction(
                PARTIAL_UNRESOLVED_IDENTIFIER, FUNCTION_2, false);

        assertThat(
                functionCatalog.lookupFunction(FULL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(IDENTIFIER), FUNCTION_2));
    }

    @Test
    public void testAmbiguousFunctionReference() throws Exception {
        // test no function is found
        assertFalse(functionCatalog.lookupFunction(PARTIAL_UNRESOLVED_IDENTIFIER).isPresent());

        // test catalog function is found
        catalog.createFunction(
                IDENTIFIER.toObjectPath(),
                new CatalogFunctionImpl(FUNCTION_1.getClass().getName()),
                false);

        assertThat(
                functionCatalog.lookupFunction(PARTIAL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(IDENTIFIER), FUNCTION_1));

        // test temporary catalog function is found
        functionCatalog.registerTemporaryCatalogFunction(
                PARTIAL_UNRESOLVED_IDENTIFIER, FUNCTION_2, false);

        assertThat(
                functionCatalog.lookupFunction(PARTIAL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(IDENTIFIER), FUNCTION_2));

        // test system function is found
        moduleManager.loadModule("test_module", new TestModule());

        assertThat(
                functionCatalog.lookupFunction(PARTIAL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(NAME), FUNCTION_3));

        // test temporary system function is found
        functionCatalog.registerTemporarySystemFunction(NAME, FUNCTION_4, false);

        assertThat(
                functionCatalog.lookupFunction(PARTIAL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(NAME), FUNCTION_4));
    }

    @Test
    public void testTemporarySystemFunction() {
        // register first time
        functionCatalog.registerTemporarySystemFunction(NAME, FUNCTION_1, false);
        assertThat(
                functionCatalog.lookupFunction(PARTIAL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(NAME), FUNCTION_1));

        // register second time lenient
        functionCatalog.registerTemporarySystemFunction(NAME, FUNCTION_2, true);
        assertThat(
                functionCatalog.lookupFunction(PARTIAL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(NAME), FUNCTION_1));

        // register second time not lenient
        try {
            functionCatalog.registerTemporarySystemFunction(NAME, FUNCTION_2, false);
            fail();
        } catch (ValidationException e) {
            assertThat(
                    e,
                    hasMessage(
                            containsString("A function named '" + NAME + "' does already exist.")));
        }

        // drop first time
        assertThat(functionCatalog.dropTemporarySystemFunction(NAME, false), equalTo(true));
        assertThat(
                functionCatalog.lookupFunction(PARTIAL_UNRESOLVED_IDENTIFIER), returnsNoFunction());

        // drop second time lenient
        assertThat(functionCatalog.dropTemporarySystemFunction(NAME, true), equalTo(false));

        // drop second time not lenient
        try {
            functionCatalog.dropTemporarySystemFunction(NAME, false);
            fail();
        } catch (ValidationException e) {
            assertThat(
                    e,
                    hasMessage(containsString("A function named '" + NAME + "' doesn't exist.")));
        }

        // register invalid
        try {
            functionCatalog.registerTemporarySystemFunction(NAME, FUNCTION_INVALID, false);
            fail();
        } catch (ValidationException e) {
            assertThat(
                    e,
                    hasMessage(
                            containsString(
                                    "Could not register temporary system function '"
                                            + NAME
                                            + "' due to implementation errors.")));
        }
    }

    @Test
    public void testUninstantiatedTemporarySystemFunction() {
        // register first time
        functionCatalog.registerTemporarySystemFunction(
                NAME, FUNCTION_1.getClass().getName(), FunctionLanguage.JAVA, false);
        assertThat(
                functionCatalog.lookupFunction(PARTIAL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(NAME), FUNCTION_1));

        // register second time lenient
        functionCatalog.registerTemporarySystemFunction(
                NAME, FUNCTION_2.getClass().getName(), FunctionLanguage.JAVA, true);
        assertThat(
                functionCatalog.lookupFunction(PARTIAL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(NAME), FUNCTION_1));

        // register second time not lenient
        try {
            functionCatalog.registerTemporarySystemFunction(
                    NAME, FUNCTION_2.getClass().getName(), FunctionLanguage.JAVA, false);
            fail();
        } catch (ValidationException e) {
            assertThat(
                    e,
                    hasMessage(
                            containsString("A function named '" + NAME + "' does already exist.")));
        }

        // register invalid
        try {
            functionCatalog.registerTemporarySystemFunction(
                    NAME, FUNCTION_INVALID.getClass().getName(), FunctionLanguage.JAVA, false);
            fail();
        } catch (ValidationException e) {
            assertThat(
                    e,
                    hasMessage(
                            containsString(
                                    "Could not register temporary system function '"
                                            + NAME
                                            + "' due to implementation errors.")));
        }

        functionCatalog.dropTemporarySystemFunction(NAME, true);

        // test register uninstantiated table function
        functionCatalog.registerTemporarySystemFunction(
                NAME, TABLE_FUNCTION.getClass().getName(), FunctionLanguage.JAVA, false);
        assertThat(
                functionCatalog.lookupFunction(PARTIAL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(NAME), TABLE_FUNCTION));

        functionCatalog.dropTemporarySystemFunction(NAME, true);

        // test register uninstantiated aggregate function
        functionCatalog.registerTemporarySystemFunction(
                NAME, AGGREGATE_FUNCTION.getClass().getName(), FunctionLanguage.JAVA, false);
        assertThat(
                functionCatalog.lookupFunction(PARTIAL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(NAME), AGGREGATE_FUNCTION));
    }

    @Test
    public void testCatalogFunction() {
        // register first time
        functionCatalog.registerCatalogFunction(
                PARTIAL_UNRESOLVED_IDENTIFIER, FUNCTION_1.getClass(), false);
        assertThat(
                functionCatalog.lookupFunction(FULL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(IDENTIFIER), FUNCTION_1));

        // register second time lenient
        functionCatalog.registerCatalogFunction(
                PARTIAL_UNRESOLVED_IDENTIFIER, FUNCTION_2.getClass(), true);
        assertThat(
                functionCatalog.lookupFunction(FULL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(IDENTIFIER), FUNCTION_1));

        // register second time not lenient
        try {
            functionCatalog.registerCatalogFunction(
                    PARTIAL_UNRESOLVED_IDENTIFIER, FUNCTION_2.getClass(), false);
            fail();
        } catch (ValidationException e) {
            assertThat(
                    e,
                    hasMessage(
                            containsString(
                                    "A function '"
                                            + IDENTIFIER.asSummaryString()
                                            + "' does already exist.")));
        }

        // drop first time
        assertThat(
                functionCatalog.dropCatalogFunction(PARTIAL_UNRESOLVED_IDENTIFIER, false),
                equalTo(true));
        assertThat(functionCatalog.lookupFunction(FULL_UNRESOLVED_IDENTIFIER), returnsNoFunction());

        // drop second time lenient
        assertThat(
                functionCatalog.dropCatalogFunction(PARTIAL_UNRESOLVED_IDENTIFIER, true),
                equalTo(false));

        // drop second time not lenient
        try {
            functionCatalog.dropCatalogFunction(PARTIAL_UNRESOLVED_IDENTIFIER, false);
            fail();
        } catch (ValidationException e) {
            assertThat(
                    e,
                    hasMessage(
                            containsString(
                                    "A function '"
                                            + IDENTIFIER.asSummaryString()
                                            + "' doesn't exist.")));
        }

        // register invalid
        try {
            functionCatalog.registerCatalogFunction(
                    PARTIAL_UNRESOLVED_IDENTIFIER, FUNCTION_INVALID.getClass(), false);
            fail();
        } catch (ValidationException e) {
            assertThat(
                    e,
                    hasMessage(
                            containsString(
                                    "Could not register catalog function '"
                                            + IDENTIFIER.asSummaryString()
                                            + "' due to implementation errors.")));
        }
    }

    @Test
    public void testTemporaryCatalogFunction() {
        // register permanent function
        functionCatalog.registerCatalogFunction(
                PARTIAL_UNRESOLVED_IDENTIFIER, FUNCTION_2.getClass(), false);
        assertThat(
                functionCatalog.lookupFunction(FULL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(IDENTIFIER), FUNCTION_2));

        // register temporary first time
        functionCatalog.registerTemporaryCatalogFunction(
                PARTIAL_UNRESOLVED_IDENTIFIER, FUNCTION_1, false);
        assertThat(
                functionCatalog.lookupFunction(FULL_UNRESOLVED_IDENTIFIER),
                returnsFunction(
                        FunctionIdentifier.of(IDENTIFIER),
                        FUNCTION_1)); // temporary function hides catalog function

        // dropping catalog functions is not possible in this state
        try {
            functionCatalog.dropCatalogFunction(PARTIAL_UNRESOLVED_IDENTIFIER, true);
            fail();
        } catch (ValidationException e) {
            assertThat(
                    e,
                    hasMessage(
                            containsString(
                                    "A temporary function '"
                                            + IDENTIFIER.asSummaryString()
                                            + "' does already exist. "
                                            + "Please drop the temporary function first.")));
        }

        // registering catalog functions is not possible in this state
        try {
            functionCatalog.registerCatalogFunction(
                    PARTIAL_UNRESOLVED_IDENTIFIER, FUNCTION_2.getClass(), false);
            fail();
        } catch (ValidationException e) {
            assertThat(
                    e,
                    hasMessage(
                            containsString(
                                    "A temporary function '"
                                            + IDENTIFIER.asSummaryString()
                                            + "' does already exist. "
                                            + "Please drop the temporary function first.")));
        }

        // register temporary second time lenient
        functionCatalog.registerTemporaryCatalogFunction(
                PARTIAL_UNRESOLVED_IDENTIFIER, FUNCTION_1, true);
        assertThat(
                functionCatalog.lookupFunction(FULL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(IDENTIFIER), FUNCTION_1));

        // register temporary second time not lenient
        try {
            functionCatalog.registerTemporaryCatalogFunction(
                    PARTIAL_UNRESOLVED_IDENTIFIER, FUNCTION_2, false);
            fail();
        } catch (ValidationException e) {
            assertThat(
                    e,
                    hasMessage(
                            containsString(
                                    "A function '"
                                            + IDENTIFIER.asSummaryString()
                                            + "' does already exist.")));
        }

        // drop temporary first time
        assertThat(
                functionCatalog.dropTemporaryCatalogFunction(PARTIAL_UNRESOLVED_IDENTIFIER, false),
                equalTo(true));
        assertThat(
                functionCatalog.lookupFunction(FULL_UNRESOLVED_IDENTIFIER),
                returnsFunction(
                        FunctionIdentifier.of(IDENTIFIER),
                        FUNCTION_2)); // permanent function is visible again

        // drop temporary second time lenient
        assertThat(
                functionCatalog.dropTemporaryCatalogFunction(PARTIAL_UNRESOLVED_IDENTIFIER, true),
                equalTo(false));

        // drop temporary second time not lenient
        try {
            functionCatalog.dropTemporaryCatalogFunction(PARTIAL_UNRESOLVED_IDENTIFIER, false);
            fail();
        } catch (ValidationException e) {
            assertThat(
                    e,
                    hasMessage(
                            containsString(
                                    "Temporary catalog function "
                                            + IDENTIFIER.toString()
                                            + " doesn't exist")));
        }

        // register invalid
        try {
            functionCatalog.registerTemporaryCatalogFunction(
                    PARTIAL_UNRESOLVED_IDENTIFIER, FUNCTION_INVALID, false);
            fail();
        } catch (ValidationException e) {
            assertThat(
                    e,
                    hasMessage(
                            containsString(
                                    "Could not register temporary catalog function '"
                                            + IDENTIFIER.asSummaryString()
                                            + "' due to implementation errors.")));
        }
    }

    @Test
    public void testUninstantiatedTemporaryCatalogFunction() {
        // register permanent function
        functionCatalog.registerCatalogFunction(
                PARTIAL_UNRESOLVED_IDENTIFIER, FUNCTION_2.getClass(), false);
        assertThat(
                functionCatalog.lookupFunction(FULL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(IDENTIFIER), FUNCTION_2));

        // register temporary first time
        functionCatalog.registerTemporaryCatalogFunction(
                PARTIAL_UNRESOLVED_IDENTIFIER,
                new CatalogFunctionImpl(FUNCTION_1.getClass().getName()),
                false);
        // temporary function hides catalog function
        assertThat(
                functionCatalog.lookupFunction(FULL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(IDENTIFIER), FUNCTION_1));

        // register temporary second time lenient
        functionCatalog.registerTemporaryCatalogFunction(
                PARTIAL_UNRESOLVED_IDENTIFIER,
                new CatalogFunctionImpl(FUNCTION_1.getClass().getName()),
                true);
        assertThat(
                functionCatalog.lookupFunction(FULL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(IDENTIFIER), FUNCTION_1));

        // register temporary second time not lenient
        try {
            functionCatalog.registerTemporaryCatalogFunction(
                    PARTIAL_UNRESOLVED_IDENTIFIER,
                    new CatalogFunctionImpl(FUNCTION_2.getClass().getName()),
                    false);
            fail();
        } catch (ValidationException e) {
            assertThat(
                    e,
                    hasMessage(
                            containsString(
                                    "A function '"
                                            + IDENTIFIER.asSummaryString()
                                            + "' does already exist.")));
        }

        // register invalid
        try {
            // drop it first to make sure function class gets validated
            functionCatalog.dropTemporaryCatalogFunction(PARTIAL_UNRESOLVED_IDENTIFIER, true);
            functionCatalog.registerTemporaryCatalogFunction(
                    PARTIAL_UNRESOLVED_IDENTIFIER,
                    new CatalogFunctionImpl(FUNCTION_INVALID.getClass().getName()),
                    false);
            fail();
        } catch (ValidationException e) {
            assertThat(
                    e,
                    hasMessage(
                            containsString(
                                    "Could not register temporary catalog function '"
                                            + IDENTIFIER.asSummaryString()
                                            + "' due to implementation errors.")));
        }

        functionCatalog.dropTemporaryCatalogFunction(PARTIAL_UNRESOLVED_IDENTIFIER, true);

        // test register uninstantiated table function
        functionCatalog.registerTemporaryCatalogFunction(
                PARTIAL_UNRESOLVED_IDENTIFIER,
                new CatalogFunctionImpl(TABLE_FUNCTION.getClass().getName()),
                false);
        assertThat(
                functionCatalog.lookupFunction(PARTIAL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(IDENTIFIER), TABLE_FUNCTION));

        functionCatalog.dropTemporaryCatalogFunction(PARTIAL_UNRESOLVED_IDENTIFIER, true);

        // test register uninstantiated aggregate function
        functionCatalog.registerTemporaryCatalogFunction(
                PARTIAL_UNRESOLVED_IDENTIFIER,
                new CatalogFunctionImpl(AGGREGATE_FUNCTION.getClass().getName()),
                false);
        assertThat(
                functionCatalog.lookupFunction(PARTIAL_UNRESOLVED_IDENTIFIER),
                returnsFunction(FunctionIdentifier.of(IDENTIFIER), AGGREGATE_FUNCTION));
    }

    // --------------------------------------------------------------------------------------------
    // Test utilities
    // --------------------------------------------------------------------------------------------

    private static Matcher<Optional<FunctionLookup.Result>> returnsFunction(
            FunctionIdentifier identifier, FunctionDefinition definition) {
        return equalTo(Optional.of(new FunctionLookup.Result(identifier, definition)));
    }

    private static Matcher<Optional<FunctionLookup.Result>> returnsNoFunction() {
        return equalTo(Optional.empty());
    }

    // --------------------------------------------------------------------------------------------
    // Test classes
    // --------------------------------------------------------------------------------------------

    private static class TestModule implements Module {

        @Override
        public Set<String> listFunctions() {
            return new HashSet<String>() {
                {
                    add(NAME);
                }
            };
        }

        @Override
        public Optional<FunctionDefinition> getFunctionDefinition(String name) {
            return Optional.of(FUNCTION_3);
        }
    }

    /** Testing function. */
    public static class TestFunction1 extends ScalarFunction {
        public String eval() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            return o != null && o.getClass() == this.getClass();
        }
    }

    /** Testing function. */
    public static class TestFunction2 extends ScalarFunction {
        public String eval() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            return o != null && o.getClass() == this.getClass();
        }
    }

    /** Testing function. */
    public static class TestFunction3 extends ScalarFunction {
        public String eval() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            return o != null && o.getClass() == this.getClass();
        }
    }

    /** Testing function. */
    public static class TestFunction4 extends ScalarFunction {
        public String eval() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            return o != null && o.getClass() == this.getClass();
        }
    }

    /** Invalid testing function. */
    public static class InvalidTestFunction extends ScalarFunction {
        // missing implementation
    }

    /** Testing table function. */
    @SuppressWarnings("unused")
    public static class TestTableFunction extends TableFunction<String> {
        public void eval(String in) {}

        @Override
        public boolean equals(Object o) {
            return o != null && o.getClass() == this.getClass();
        }
    }

    /** Testing aggregate function. */
    @SuppressWarnings("unused")
    public static class TestAggregateFunction extends AggregateFunction<String, String> {

        @Override
        public String getValue(String accumulator) {
            return null;
        }

        @Override
        public String createAccumulator() {
            return null;
        }

        public void accumulate(String in) {}

        @Override
        public boolean equals(Object o) {
            return o != null && o.getClass() == this.getClass();
        }
    }
}
