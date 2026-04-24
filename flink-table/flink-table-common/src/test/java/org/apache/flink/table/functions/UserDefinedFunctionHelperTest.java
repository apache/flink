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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.resource.ResourceUri;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.functions.UserDefinedFunctionHelper.instantiateFunction;
import static org.apache.flink.table.functions.UserDefinedFunctionHelper.isClassNameSerializable;
import static org.apache.flink.table.functions.UserDefinedFunctionHelper.prepareInstance;
import static org.apache.flink.table.functions.UserDefinedFunctionHelper.validateAsyncTableFunctionTimeoutClass;
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

    // --------------------------------------------------------------------------------------------
    // Tests for validateAsyncTableFunctionTimeoutClass (T017 / T020)
    // --------------------------------------------------------------------------------------------

    /** US2 (T017): absent / private / static / misspelled timeout methods return false. */
    @Test
    void testValidateAsyncTimeoutReturnsFalseWhenNoApplicableMethod() {
        final Class<?>[] argumentClasses = new Class<?>[] {Integer.class};

        assertThat(
                        validateAsyncTableFunctionTimeoutClass(
                                NoTimeoutAsyncTableFunction.class, argumentClasses, "f"))
                .isFalse();
        assertThat(
                        validateAsyncTableFunctionTimeoutClass(
                                PrivateTimeoutAsyncTableFunction.class, argumentClasses, "f"))
                .isFalse();
        assertThat(
                        validateAsyncTableFunctionTimeoutClass(
                                StaticTimeoutAsyncTableFunction.class, argumentClasses, "f"))
                .isFalse();
        assertThat(
                        validateAsyncTableFunctionTimeoutClass(
                                MisspelledTimeoutAsyncTableFunction.class, argumentClasses, "f"))
                .isFalse();
    }

    /** US3 (T020): matching signature returns true. */
    @Test
    void testValidateAsyncTimeoutAcceptsMatchingSignature() {
        final Class<?>[] argumentClasses = new Class<?>[] {Integer.class};
        assertThat(
                        validateAsyncTableFunctionTimeoutClass(
                                ValidTimeoutAsyncTableFunction.class, argumentClasses, "f"))
                .isTrue();
    }

    /** US3 (T020): wrong arg count fails fast with FQN + expected + actual signature. */
    @Test
    void testValidateAsyncTimeoutRejectsWrongArgCount() {
        final Class<?>[] argumentClasses = new Class<?>[] {Integer.class, String.class};
        assertThatThrownBy(
                        () ->
                                validateAsyncTableFunctionTimeoutClass(
                                        WrongArgCountTimeoutAsyncTableFunction.class,
                                        argumentClasses,
                                        "f"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(WrongArgCountTimeoutAsyncTableFunction.class.getName())
                .hasMessageContaining(Integer.class.getName())
                .hasMessageContaining(String.class.getName());
    }

    /** US3 (T020): wrong arg type fails fast with FQN + expected + actual signature. */
    @Test
    void testValidateAsyncTimeoutRejectsWrongArgType() {
        final Class<?>[] argumentClasses = new Class<?>[] {Integer.class};
        assertThatThrownBy(
                        () ->
                                validateAsyncTableFunctionTimeoutClass(
                                        WrongArgTypeTimeoutAsyncTableFunction.class,
                                        argumentClasses,
                                        "f"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(WrongArgTypeTimeoutAsyncTableFunction.class.getName())
                .hasMessageContaining(Integer.class.getName())
                .hasMessageContaining(String.class.getName());
    }

    /** US3 (T020): first arg not CompletableFuture fails fast. */
    @Test
    void testValidateAsyncTimeoutRejectsMissingFutureArg() {
        final Class<?>[] argumentClasses = new Class<?>[] {Integer.class};
        assertThatThrownBy(
                        () ->
                                validateAsyncTableFunctionTimeoutClass(
                                        NoFutureTimeoutAsyncTableFunction.class,
                                        argumentClasses,
                                        "f"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(NoFutureTimeoutAsyncTableFunction.class.getName());
    }

    /** US3 (T020): timeout must return void. */
    @Test
    void testValidateAsyncTimeoutRejectsNonVoidReturnType() {
        final Class<?>[] argumentClasses = new Class<?>[] {Integer.class};
        assertThatThrownBy(
                        () ->
                                validateAsyncTableFunctionTimeoutClass(
                                        NonVoidReturnTimeoutAsyncTableFunction.class,
                                        argumentClasses,
                                        "f"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(NonVoidReturnTimeoutAsyncTableFunction.class.getName())
                .hasMessageContaining("java.lang.String");
    }

    /** US3 (T020): raw CompletableFuture is rejected because generic parameters are required. */
    @Test
    void testValidateAsyncTimeoutRejectsRawFutureType() {
        final Class<?>[] argumentClasses = new Class<?>[] {Integer.class};
        assertThatThrownBy(
                        () ->
                                validateAsyncTableFunctionTimeoutClass(
                                        RawFutureTimeoutAsyncTableFunction.class,
                                        argumentClasses,
                                        "f"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(RawFutureTimeoutAsyncTableFunction.class.getName())
                .hasMessageContaining("CompletableFuture")
                .hasMessageContaining("Collection<T>");
    }

    /** US3 (T020): timeout future element type must match the paired eval overload. */
    @Test
    void testValidateAsyncTimeoutRejectsMismatchedFutureElementType() {
        final Class<?>[] argumentClasses = new Class<?>[] {Integer.class};
        assertThatThrownBy(
                        () ->
                                validateAsyncTableFunctionTimeoutClass(
                                        MismatchedFutureElementTimeoutAsyncTableFunction.class,
                                        argumentClasses,
                                        "f"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        MismatchedFutureElementTimeoutAsyncTableFunction.class.getName())
                .hasMessageContaining("matching the corresponding eval");
    }

    /**
     * Multiple {@code timeout} overloads coexist — the one whose parameter list matches the call
     * site's keys must be resolved as valid, without being shadowed by the other overload.
     */
    @Test
    void testValidateAsyncTimeoutResolvesAmongOverloads() {
        assertThat(
                        validateAsyncTableFunctionTimeoutClass(
                                MultipleOverloadTimeoutAsyncTableFunction.class,
                                new Class<?>[] {Integer.class},
                                "f"))
                .isTrue();
    }

    /**
     * Multiple {@code timeout} overloads are declared but none matches the call site's keys — the
     * error message must still include the function class, the expected key type, and evidence that
     * timeout overloads were examined.
     */
    @Test
    void testValidateAsyncTimeoutListsAllCandidatesWhenNoOverloadMatches() {
        assertThatThrownBy(
                        () ->
                                validateAsyncTableFunctionTimeoutClass(
                                        MultipleOverloadTimeoutAsyncTableFunction.class,
                                        new Class<?>[] {Long.class},
                                        "f"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(MultipleOverloadTimeoutAsyncTableFunction.class.getName())
                .hasMessageContaining(Long.class.getName())
                .hasMessageContaining("timeout")
                .hasMessageContaining(Integer.class.getName())
                .hasMessageContaining(String.class.getName());
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
                TestSpec.forClass(ValidAsyncScalarFunction.class).expectSuccess(),
                TestSpec.forClass(ValidAsyncTableFunction.class).expectSuccess(),
                TestSpec.forInstance(new ValidAsyncScalarFunction()).expectSuccess(),
                TestSpec.forClass(PrivateAsyncScalarFunction.class)
                        .expectErrorMessage(
                                "Function class '"
                                        + PrivateAsyncScalarFunction.class.getName()
                                        + "' is not public."),
                TestSpec.forClass(PrivateAsyncTableFunction.class)
                        .expectErrorMessage(
                                "Function class '"
                                        + PrivateAsyncTableFunction.class.getName()
                                        + "' is not public."),
                TestSpec.forClass(MissingImplementationAsyncScalarFunction.class)
                        .expectErrorMessage(
                                "Function class '"
                                        + MissingImplementationAsyncScalarFunction.class.getName()
                                        + "' does not implement a method named 'eval'."),
                TestSpec.forClass(MissingImplementationAsyncTableFunction.class)
                        .expectErrorMessage(
                                "Function class '"
                                        + MissingImplementationAsyncTableFunction.class.getName()
                                        + "' does not implement a method named 'eval'."),
                TestSpec.forClass(PrivateMethodAsyncScalarFunction.class)
                        .expectErrorMessage(
                                "Method 'eval' of function class '"
                                        + PrivateMethodAsyncScalarFunction.class.getName()
                                        + "' is not public."),
                TestSpec.forClass(PrivateMethodAsyncTableFunction.class)
                        .expectErrorMessage(
                                "Method 'eval' of function class '"
                                        + PrivateMethodAsyncTableFunction.class.getName()
                                        + "' is not public."),
                TestSpec.forClass(NonVoidAsyncScalarFunction.class)
                        .expectErrorMessage(
                                "Method 'eval' of function class '"
                                        + NonVoidAsyncScalarFunction.class.getName()
                                        + "' must be void."),
                TestSpec.forClass(NonVoidAsyncTableFunction.class)
                        .expectErrorMessage(
                                "Method 'eval' of function class '"
                                        + NonVoidAsyncTableFunction.class.getName()
                                        + "' must be void."),
                TestSpec.forClass(NoFutureAsyncScalarFunction.class)
                        .expectErrorMessage(
                                "Method 'eval' of function class '"
                                        + NoFutureAsyncScalarFunction.class.getName()
                                        + "' must have a first argument of type java.util.concurrent.CompletableFuture."),
                TestSpec.forClass(NoFutureAsyncTableFunction.class)
                        .expectErrorMessage(
                                "Method 'eval' of function class '"
                                        + NoFutureAsyncTableFunction.class.getName()
                                        + "' must have a first argument of type java.util.concurrent.CompletableFuture<java.util.Collection>."),
                TestSpec.forClass(NoFutureAsyncTableFunction2.class)
                        .expectErrorMessage(
                                "Method 'eval' of function class '"
                                        + NoFutureAsyncTableFunction2.class.getName()
                                        + "' must have a first argument of type java.util.concurrent.CompletableFuture<java.util.Collection>."),
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

    /** Valid scalar function. */
    public static class ValidAsyncScalarFunction extends AsyncScalarFunction {
        public void eval(CompletableFuture<Integer> future, int i) {}
    }

    /** Valid table function. */
    public static class ValidAsyncTableFunction extends AsyncTableFunction<Integer> {
        public void eval(CompletableFuture<Collection<Integer>> future, int i) {}
    }

    private static class PrivateAsyncScalarFunction extends AsyncScalarFunction {
        public void eval(CompletableFuture<Integer> future, int i) {}
    }

    private static class PrivateAsyncTableFunction extends AsyncTableFunction<Integer> {
        public void eval(CompletableFuture<Collection<Integer>> future, int i) {}
    }

    /** No implementation method. */
    public static class MissingImplementationAsyncScalarFunction extends AsyncScalarFunction {
        // nothing to do
    }

    /** No implementation method. */
    public static class MissingImplementationAsyncTableFunction
            extends AsyncTableFunction<Integer> {
        // nothing to do
    }

    /** Implementation method is private. */
    public static class PrivateMethodAsyncScalarFunction extends AsyncScalarFunction {
        private void eval(CompletableFuture<Integer> future, int i) {}
    }

    /** Implementation method is private. */
    public static class PrivateMethodAsyncTableFunction extends AsyncTableFunction<Integer> {
        private void eval(CompletableFuture<Collection<Integer>> future, int i) {}
    }

    /** Implementation method isn't void. */
    public static class NonVoidAsyncScalarFunction extends AsyncScalarFunction {
        public String eval(CompletableFuture<Integer> future, int i) {
            return "";
        }
    }

    /** Implementation method isn't void. */
    public static class NonVoidAsyncTableFunction extends AsyncScalarFunction {
        public String eval(CompletableFuture<Collection<Integer>> future, int i) {
            return "";
        }
    }

    /** First param isn't a future. */
    public static class NoFutureAsyncScalarFunction extends AsyncScalarFunction {
        public void eval(int i) {}
    }

    /** First param isn't a future. */
    public static class NoFutureAsyncTableFunction extends AsyncTableFunction<Integer> {
        public void eval(int i) {}
    }

    /** First param is a future, but with the wrong contained type. */
    public static class NoFutureAsyncTableFunction2 extends AsyncTableFunction<Integer> {
        public void eval(CompletableFuture<Optional<Integer>> future, int i) {}
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

    // --------------------------------------------------------------------------------------------
    // AsyncTableFunction subclasses used by validateAsyncTableFunctionTimeoutClass tests
    // --------------------------------------------------------------------------------------------

    /** No timeout method declared. */
    public static class NoTimeoutAsyncTableFunction extends AsyncTableFunction<RowData> {
        public void eval(CompletableFuture<Collection<RowData>> future, Integer key) {}
    }

    /** Private timeout — must be ignored. */
    public static class PrivateTimeoutAsyncTableFunction extends AsyncTableFunction<RowData> {
        public void eval(CompletableFuture<Collection<RowData>> future, Integer key) {}

        private void timeout(CompletableFuture<Collection<RowData>> future, Integer key) {}
    }

    /** Static timeout — must be ignored. */
    public static class StaticTimeoutAsyncTableFunction extends AsyncTableFunction<RowData> {
        public void eval(CompletableFuture<Collection<RowData>> future, Integer key) {}

        public static void timeout(CompletableFuture<Collection<RowData>> future, Integer key) {}
    }

    /** Method name misspelled — must be ignored. */
    public static class MisspelledTimeoutAsyncTableFunction extends AsyncTableFunction<RowData> {
        public void eval(CompletableFuture<Collection<RowData>> future, Integer key) {}

        public void timout(CompletableFuture<Collection<RowData>> future, Integer key) {}
    }

    /** Valid timeout signature matching argumentClasses. */
    public static class ValidTimeoutAsyncTableFunction extends AsyncTableFunction<RowData> {
        public void eval(CompletableFuture<Collection<RowData>> future, Integer key) {}

        public void timeout(CompletableFuture<Collection<RowData>> future, Integer key) {}
    }

    /** Timeout exists but accepts wrong number of args. */
    public static class WrongArgCountTimeoutAsyncTableFunction extends AsyncTableFunction<RowData> {
        public void eval(
                CompletableFuture<Collection<RowData>> future, Integer key1, String key2) {}

        public void timeout(CompletableFuture<Collection<RowData>> future, Integer key) {}
    }

    /** Timeout exists but accepts wrong arg type. */
    public static class WrongArgTypeTimeoutAsyncTableFunction extends AsyncTableFunction<RowData> {
        public void eval(CompletableFuture<Collection<RowData>> future, Integer key) {}

        public void timeout(CompletableFuture<Collection<RowData>> future, String key) {}
    }

    /** Timeout first arg is not CompletableFuture. */
    public static class NoFutureTimeoutAsyncTableFunction extends AsyncTableFunction<RowData> {
        public void eval(CompletableFuture<Collection<RowData>> future, Integer key) {}

        public void timeout(Integer key) {}
    }

    /** Timeout returns a non-void type. */
    public static class NonVoidReturnTimeoutAsyncTableFunction extends AsyncTableFunction<RowData> {
        public void eval(CompletableFuture<Collection<RowData>> future, Integer key) {}

        public String timeout(CompletableFuture<Collection<RowData>> future, Integer key) {
            return "boom";
        }
    }

    /** Timeout declares a raw CompletableFuture instead of a parameterized one. */
    @SuppressWarnings("rawtypes")
    public static class RawFutureTimeoutAsyncTableFunction extends AsyncTableFunction<RowData> {
        public void eval(CompletableFuture<Collection<RowData>> future, Integer key) {}

        public void timeout(CompletableFuture future, Integer key) {}
    }

    /** Timeout declares a different Collection element type than the paired eval overload. */
    public static class MismatchedFutureElementTimeoutAsyncTableFunction
            extends AsyncTableFunction<RowData> {
        public void eval(CompletableFuture<Collection<RowData>> future, Integer key) {}

        public void timeout(CompletableFuture<Collection<String>> future, Integer key) {}
    }

    /**
     * Two {@code eval} overloads with paired {@code timeout} overloads. Used to verify both the
     * matched-overload path (returns true for either signature) and the unmatched-overload path
     * (error message must list both candidates).
     */
    public static class MultipleOverloadTimeoutAsyncTableFunction
            extends AsyncTableFunction<RowData> {
        public void eval(CompletableFuture<Collection<RowData>> future, Integer key) {}

        public void eval(CompletableFuture<Collection<RowData>> future, Integer k1, String k2) {}

        public void timeout(CompletableFuture<Collection<RowData>> future, Integer key) {}

        public void timeout(CompletableFuture<Collection<RowData>> future, Integer k1, String k2) {}
    }
}
