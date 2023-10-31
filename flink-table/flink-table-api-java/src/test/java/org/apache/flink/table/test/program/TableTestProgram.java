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

package org.apache.flink.table.test.program;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.test.program.FunctionTestStep.FunctionBehavior;
import org.apache.flink.table.test.program.FunctionTestStep.FunctionPersistence;
import org.apache.flink.table.test.program.TestStep.TestKind;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A generic declaration of a table program for testing.
 *
 * <p>A test program defines the basic test pipeline (from source to sink) and required artifacts
 * such as table sources and sinks, configuration options, and user-defined functions. Because some
 * programs need to create artifacts in a certain order, a program consists of individual {@link
 * TestStep}s for setting up the test and the actual running of the test.
 *
 * <p>Tests programs are intended to reduce code duplication and test the same SQL statement though
 * different layers of the stack. Different {@link TableTestProgramRunner}s can share the same
 * program and enrich it with custom implementation and assertions.
 *
 * <p>For example, a SQL query such as {@code SELECT * FROM (VALUES (1), (2), (3))} can be declared
 * once and can be shared among different tests for integration testing, optimizer plan testing,
 * compiled plan testing, transformation testing, and others.
 *
 * <p>A typical implementation looks like:
 *
 * <pre>{@code
 * // Define the behavior and configuration of an operation.
 * public class CalcTestPrograms {
 *     public static final TableTestProgram CALC_SIMPLE = TableTestProgram.of("calc-simple") ...;
 *     public static final TableTestProgram CALC_COMPLEX = TableTestProgram.of("calc-complex") ...;
 * }
 *
 * // Define a test base for example for plan testing
 * public abstract class PlanTestBase implements TableTestProgramRunner {
 *     // The test base declares what kind of steps it can apply.
 *     public Set<TestStep.Kind> supportedSetupSteps() { return EnumSet.of(SOURCE_WITH_DATA, SINK_WITH_DATA); }
 *     public Set<TestStep.Kind> supportedRunSteps() { return EnumSet.of(SQL); }
 *
 *     // Leave the list of programs up to the concrete test
 *     public abstract List<TableTestProgram> programs();
 *
 *     @ParameterizedTest
 *     @MethodSource("supportedPrograms")
 *     public void test(TableTestProgram program) {
 *         TableEnvironment env = ...;
 *         program.getSetupSourceTestSteps().forEach(s -> s.apply(env));
 *         program.getSetupSinkTestSteps().forEach(s -> s.apply(env));
 *         assertThat(program.getRunSqlTestStep().apply(env)).contains(...);
 *     }
 * }
 *
 * // Run the test base for a category of test programs.
 * public class CalcPlanTest extends PlanTestBase {
 *     public List<TableTestProgram> programs() = { return Arrays.asList(CALC_SIMPLE, CALC_COMPLEX); }
 * }
 * }</pre>
 */
public class TableTestProgram {

    /** Identifier of the test program (e.g. for naming generated files). */
    public final String id;

    /** Description for internal documentation. */
    public final String description;

    /** Steps to be executed for setting up an environment. */
    public final List<TestStep> setupSteps;

    /** Steps to be executed for running the actual test. */
    public final List<TestStep> runSteps;

    private TableTestProgram(
            String id, String description, List<TestStep> setupSteps, List<TestStep> runSteps) {
        this.id = id;
        this.description = description;
        this.setupSteps = setupSteps;
        this.runSteps = runSteps;
    }

    @Override
    public String toString() {
        return id;
    }

    /**
     * Entrypoint for a {@link TableTestProgram} that forces an identifier and description of the
     * test program.
     *
     * <p>The identifier is necessary to (ideally globally) identify the test program in outputs.
     * For example, a runner for plan tests can create directories and use the name as file names.
     * The identifier must start with the name of the exec node under testing.
     *
     * <p>The description should give more context and should start with a verb and "s" suffix.
     *
     * <p>For example:
     *
     * <ul>
     *   <li>TableTestProgram.of("join-outer", "tests outer joins")
     *   <li>TableTestProgram.of("rank-x-enabled", "validates a rank with config flag 'x' set")
     *   <li>TableTestProgram.of("calc-with-projection", "verifies FLINK-12345 is fixed due to
     *       missing row projection")
     * </ul>
     */
    public static Builder of(String id, String description) {
        return new Builder(id, description);
    }

    /** Convenience method to avoid casting. It assumes that the order of steps is not important. */
    public List<SourceTestStep> getSetupSourceTestSteps() {
        final EnumSet<TestKind> sourceKinds =
                EnumSet.of(
                        TestKind.SOURCE_WITHOUT_DATA,
                        TestKind.SOURCE_WITH_DATA,
                        TestKind.SOURCE_WITH_RESTORE_DATA);
        return setupSteps.stream()
                .filter(s -> sourceKinds.contains(s.getKind()))
                .map(SourceTestStep.class::cast)
                .collect(Collectors.toList());
    }

    /** Convenience method to avoid casting. It assumes that the order of steps is not important. */
    public List<SinkTestStep> getSetupSinkTestSteps() {
        final EnumSet<TestKind> sinkKinds =
                EnumSet.of(
                        TestKind.SINK_WITHOUT_DATA,
                        TestKind.SINK_WITH_DATA,
                        TestKind.SINK_WITH_RESTORE_DATA);
        return setupSteps.stream()
                .filter(s -> sinkKinds.contains(s.getKind()))
                .map(SinkTestStep.class::cast)
                .collect(Collectors.toList());
    }

    /** Convenience method to avoid casting. It assumes that the order of steps is not important. */
    public List<ConfigOptionTestStep<?>> getSetupConfigOptionTestSteps() {
        return setupSteps.stream()
                .filter(s -> s.getKind() == TestKind.CONFIG)
                .map(s -> (ConfigOptionTestStep<?>) s)
                .collect(Collectors.toList());
    }

    /** Convenience method to avoid casting. It assumes that the order of steps is not important. */
    public List<FunctionTestStep> getSetupFunctionTestSteps() {
        return setupSteps.stream()
                .filter(s -> s.getKind() == TestKind.FUNCTION)
                .map(FunctionTestStep.class::cast)
                .collect(Collectors.toList());
    }

    /**
     * Convenience method to avoid boilerplate code. It assumes that only a single SQL statement is
     * tested.
     */
    public SqlTestStep getRunSqlTestStep() {
        Preconditions.checkArgument(
                runSteps.size() == 1 && runSteps.get(0).getKind() == TestKind.SQL,
                "Single SQL step expected.");
        return (SqlTestStep) runSteps.get(0);
    }

    /** Builder pattern for {@link TableTestProgram}. */
    public static class Builder {

        private final String id;
        private final String description;
        private final List<TestStep> setupSteps = new ArrayList<>();
        private final List<TestStep> runSteps = new ArrayList<>();

        private Builder(String id, String description) {
            this.id = id;
            this.description = description;
        }

        /**
         * Setup step for execution SQL.
         *
         * <p>Note: Not every runner supports generic SQL statements. Sometimes the runner would
         * like to enrich properties e.g. of a CREATE TABLE. Use this step with caution.
         */
        public Builder setupSql(String sql) {
            this.setupSteps.add(new SqlTestStep(sql));
            return this;
        }

        /** Setup step for setting a {@link ConfigOption}. */
        public <T> Builder setupConfig(ConfigOption<T> option, T value) {
            this.setupSteps.add(new ConfigOptionTestStep<>(option, value));
            return this;
        }

        /** Setup step for registering a temporary system function. */
        public Builder setupTemporarySystemFunction(
                String name, Class<? extends UserDefinedFunction> function) {
            this.setupSteps.add(
                    new FunctionTestStep(
                            FunctionPersistence.TEMPORARY,
                            FunctionBehavior.SYSTEM,
                            name,
                            function));
            return this;
        }

        /** Setup step for registering a temporary catalog function. */
        public Builder setupTemporaryCatalogFunction(
                String name, Class<? extends UserDefinedFunction> function) {
            this.setupSteps.add(
                    new FunctionTestStep(
                            FunctionPersistence.TEMPORARY,
                            FunctionBehavior.CATALOG,
                            name,
                            function));
            return this;
        }

        /** Setup step for registering a catalog function. */
        public Builder setupCatalogFunction(
                String name, Class<? extends UserDefinedFunction> function) {
            this.setupSteps.add(
                    new FunctionTestStep(
                            FunctionPersistence.PERSISTENT,
                            FunctionBehavior.CATALOG,
                            name,
                            function));
            return this;
        }

        /**
         * Setup step for a table source.
         *
         * <p>Use {@link SourceTestStep.Builder} to construct this step.
         */
        public Builder setupTableSource(SourceTestStep sourceTestStep) {
            setupSteps.add(sourceTestStep);
            return this;
        }

        /**
         * Setup step for a table sink.
         *
         * <p>Use {@link SinkTestStep.Builder} to construct this step.
         */
        public Builder setupTableSink(SinkTestStep sinkTestStep) {
            setupSteps.add(sinkTestStep);
            return this;
        }

        /** Run step for executing SQL. */
        public Builder runSql(String sql) {
            this.runSteps.add(new SqlTestStep(sql));
            return this;
        }

        /** Run step for executing a statement set. */
        public Builder runStatementSet(String... sql) {
            this.runSteps.add(new StatementSetTestStep(Arrays.asList(sql)));
            return this;
        }

        public TableTestProgram build() {
            return new TableTestProgram(id, description, setupSteps, runSteps);
        }
    }
}
