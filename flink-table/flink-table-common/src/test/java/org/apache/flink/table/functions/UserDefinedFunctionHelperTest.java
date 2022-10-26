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

package org.apache.flink.table.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.resource.ResourceUri;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.functions.UserDefinedFunctionHelper.instantiateFunction;
import static org.apache.flink.table.functions.UserDefinedFunctionHelper.isClassNameSerializable;
import static org.apache.flink.table.functions.UserDefinedFunctionHelper.prepareInstance;
import static org.apache.flink.table.functions.UserDefinedFunctionHelper.validateClass;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link UserDefinedFunctionHelper}. */
@SuppressWarnings("unused")
class UserDefinedFunctionHelperTest {

    @ParameterizedTest
    @MethodSource("testSpecs")
    void testInstantiation(TestSpec testSpec) {
        final Supplier<UserDefinedFunction> supplier;
        if (testSpec.functionClass != null) {
            supplier = () -> instantiateFunction(testSpec.functionClass);
        } else if (testSpec.catalogFunction != null) {
            supplier =
                    () ->
                            instantiateFunction(
                                    UserDefinedFunctionHelperTest.class.getClassLoader(),
                                    new Configuration(),
                                    "f",
                                    testSpec.catalogFunction);
        } else {
            return;
        }

        if (testSpec.expectedErrorMessage != null) {
            assertThatThrownBy(supplier::get)
                    .satisfies(
                            anyCauseMatches(
                                    ValidationException.class, testSpec.expectedErrorMessage));
        } else {
            assertThat(supplier.get()).isNotNull();
        }
    }

    @ParameterizedTest
    @MethodSource("testSpecs")
    void testValidation(TestSpec testSpec) {
        final Runnable runnable;
        if (testSpec.functionClass != null) {
            runnable = () -> validateClass(testSpec.functionClass);
        } else if (testSpec.functionInstance != null) {
            runnable = () -> prepareInstance(new Configuration(), testSpec.functionInstance);
        } else {
            return;
        }

        if (testSpec.expectedErrorMessage != null) {
            assertThatThrownBy(runnable::run)
                    .satisfies(
                            anyCauseMatches(
                                    ValidationException.class, testSpec.expectedErrorMessage));
        } else {
            runnable.run();
        }
    }

    @Test
    void testSerialization() {
        assertThat(isClassNameSerializable(new ValidTableFunction())).isTrue();

        assertThat(isClassNameSerializable(new ValidScalarFunction())).isTrue();

        assertThat(isClassNameSerializable(new ParameterizedTableFunction(12))).isFalse();

        assertThat(isClassNameSerializable(new StatefulScalarFunction())).isFalse();
    }

    // --------------------------------------------------------------------------------------------
    // Test utilities
    // --------------------------------------------------------------------------------------------

    private static List<TestSpec> testSpecs() {
        return Arrays.asList(
                TestSpec.forClass(ValidScalarFunction.class).expectSuccess(),
                TestSpec.forInstance(new ValidScalarFunction()).expectSuccess(),
                TestSpec.forClass(PrivateScalarFunction.class)
                        .expectErrorMessage(
                                "Function class '"
                                        + PrivateScalarFunction.class.getName()
                                        + "' is not public."),
                TestSpec.forClass(MissingImplementationScalarFunction.class)
                        .expectErrorMessage(
                                "Function class '"
                                        + MissingImplementationScalarFunction.class.getName()
                                        + "' does not implement a method named 'eval'."),
                TestSpec.forClass(PrivateMethodScalarFunction.class)
                        .expectErrorMessage(
                                "Method 'eval' of function class '"
                                        + PrivateMethodScalarFunction.class.getName()
                                        + "' is not public."),
                TestSpec.forInstance(new ValidTableAggregateFunction()).expectSuccess(),
                TestSpec.forInstance(new MissingEmitTableAggregateFunction())
                        .expectErrorMessage(
                                "Function class '"
                                        + MissingEmitTableAggregateFunction.class.getName()
                                        + "' does not implement a method named 'emitUpdateWithRetract' or 'emitValue'."),
                TestSpec.forInstance(new ValidTableFunction()).expectSuccess(),
                TestSpec.forInstance(new ParameterizedTableFunction(12)).expectSuccess(),
                TestSpec.forClass(ParameterizedTableFunction.class)
                        .expectErrorMessage(
                                "Function class '"
                                        + ParameterizedTableFunction.class.getName()
                                        + "' must have a public default constructor."),
                TestSpec.forClass(HierarchicalTableAggregateFunction.class).expectSuccess(),
                TestSpec.forCatalogFunction(ValidScalarFunction.class.getName()).expectSuccess(),
                TestSpec.forCatalogFunction("I don't exist.")
                        .expectErrorMessage("Cannot instantiate user-defined function 'f'."));
    }

    private static class TestSpec {

        final @Nullable Class<? extends UserDefinedFunction> functionClass;

        final @Nullable UserDefinedFunction functionInstance;

        final @Nullable CatalogFunction catalogFunction;

        @Nullable String expectedErrorMessage;

        TestSpec(
                @Nullable Class<? extends UserDefinedFunction> functionClass,
                @Nullable UserDefinedFunction functionInstance,
                @Nullable CatalogFunction catalogFunction) {
            this.functionClass = functionClass;
            this.functionInstance = functionInstance;
            this.catalogFunction = catalogFunction;
        }

        static TestSpec forClass(Class<? extends UserDefinedFunction> function) {
            return new TestSpec(function, null, null);
        }

        static TestSpec forInstance(UserDefinedFunction function) {
            return new TestSpec(null, function, null);
        }

        static TestSpec forCatalogFunction(String className) {
            return new TestSpec(null, null, new CatalogFunctionMock(className));
        }

        TestSpec expectErrorMessage(String expectedErrorMessage) {
            this.expectedErrorMessage = expectedErrorMessage;
            return this;
        }

        TestSpec expectSuccess() {
            this.expectedErrorMessage = null;
            return this;
        }
    }

    private static class CatalogFunctionMock implements CatalogFunction {

        private final String className;

        CatalogFunctionMock(String className) {
            this.className = className;
        }

        @Override
        public String getClassName() {
            return className;
        }

        @Override
        public CatalogFunction copy() {
            return null;
        }

        @Override
        public Optional<String> getDescription() {
            return Optional.empty();
        }

        @Override
        public Optional<String> getDetailedDescription() {
            return Optional.empty();
        }

        @Override
        public boolean isGeneric() {
            return false;
        }

        @Override
        public FunctionLanguage getFunctionLanguage() {
            return FunctionLanguage.JAVA;
        }

        @Override
        public List<ResourceUri> getFunctionResources() {
            return Collections.emptyList();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Test classes for validation
    // --------------------------------------------------------------------------------------------

    /** Valid scalar function. */
    public static class ValidScalarFunction extends ScalarFunction {
        public String eval(int i) {
            return null;
        }
    }

    private static class PrivateScalarFunction extends ScalarFunction {
        public String eval(int i) {
            return null;
        }
    }

    /** No implementation method. */
    public static class MissingImplementationScalarFunction extends ScalarFunction {
        // nothing to do
    }

    /** Implementation method is private. */
    public static class PrivateMethodScalarFunction extends ScalarFunction {
        private String eval(int i) {
            return null;
        }
    }

    /** Valid table aggregate function. */
    public static class ValidTableAggregateFunction extends TableAggregateFunction<String, String> {

        public void accumulate(String acc, String in) {
            // nothing to do
        }

        public void emitValue(String acc, Collector<String> out) {
            // nothing to do
        }

        @Override
        public String createAccumulator() {
            return null;
        }
    }

    /** No emitting method implementation. */
    public static class MissingEmitTableAggregateFunction
            extends TableAggregateFunction<String, String> {

        public void accumulate(String acc, String in) {
            // nothing to do
        }

        @Override
        public String createAccumulator() {
            return null;
        }
    }

    /** Valid table function. */
    public static class ValidTableFunction extends TableFunction<String> {
        public void eval(String i) {
            // nothing to do
        }
    }

    /** Table function with parameters in constructor. */
    public static class ParameterizedTableFunction extends TableFunction<String> {

        public ParameterizedTableFunction(int param) {
            // nothing to do
        }

        public void eval(String i) {
            // nothing to do
        }
    }

    private abstract static class AbstractTableAggregateFunction
            extends TableAggregateFunction<String, String> {
        public void accumulate(String acc, String in) {
            // nothing to do
        }
    }

    /** Hierarchy that is implementing different methods. */
    public static class HierarchicalTableAggregateFunction extends AbstractTableAggregateFunction {

        public void emitValue(String acc, Collector<String> out) {
            // nothing to do
        }

        @Override
        public String createAccumulator() {
            return null;
        }
    }

    /** Function with state. */
    public static class StatefulScalarFunction extends ScalarFunction {
        public String state;

        public String eval() {
            return state;
        }
    }
}
