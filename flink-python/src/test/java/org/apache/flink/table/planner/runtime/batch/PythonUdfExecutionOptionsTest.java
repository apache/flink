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

package org.apache.flink.table.planner.runtime.batch;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.AsyncPythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PandasScalarFunction;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for applying per-UDF execution options to translated Python calc operators. */
class PythonUdfExecutionOptionsTest extends TableTestBase {

    private BatchTableTestUtil util;

    @BeforeEach
    void setUp() {
        util = batchTestUtil(TableConfig.getDefault());
        util.getTableEnv()
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE source_table (a INT, b INT) WITH ("
                                + "'connector' = 'filesystem', "
                                + "'format' = 'testcsv', "
                                + "'path' = '/tmp')");
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE sink_table (a INT, b INT) WITH ("
                                + "'connector' = 'values')");
    }

    @Test
    void testPandasOptionsSetParallelismAndMinimumBatchSize() throws Exception {
        util.addTemporarySystemFunction(
                "pandas64", new ResourcePandasScalarFunction("pandas64", 2, 64));
        util.addTemporarySystemFunction(
                "pandas32", new ResourcePandasScalarFunction("pandas32", 2, 32));

        final OneInputTransformation<?, ?> pythonCalc =
                getPythonTransformation(
                        "INSERT INTO sink_table "
                                + "SELECT pandas64(a, b), pandas32(a, b) FROM source_table");

        assertThat(pythonCalc.getParallelism()).isEqualTo(2);
        assertThat(pythonCalc.isParallelismConfigured()).isTrue();
        assertThat(getPythonOperatorConfig(pythonCalc))
                .returns(32, config -> config.get(PythonOptions.MAX_ARROW_BATCH_SIZE));
    }

    @Test
    void testUnspecifiedParallelismInheritsInput() throws Exception {
        util.getTableEnv().getConfig().set(PythonOptions.MAX_ARROW_BATCH_SIZE, 257);
        util.addTemporarySystemFunction(
                "pandas_default", new ResourcePandasScalarFunction("pandas_default", -1, -1));

        final OneInputTransformation<?, ?> pythonCalc =
                getPythonTransformation(
                        "INSERT INTO sink_table "
                                + "SELECT pandas_default(a, b), b FROM source_table");

        assertThat(pythonCalc.getParallelism()).isEqualTo(4);
        assertThat(pythonCalc.isParallelismConfigured()).isFalse();
        assertThat(getPythonOperatorConfig(pythonCalc))
                .returns(257, config -> config.get(PythonOptions.MAX_ARROW_BATCH_SIZE));
    }

    @Test
    void testAsyncConcurrencySetsConfiguredParallelism() {
        util.addTemporarySystemFunction(
                "async_func", new ResourceAsyncPythonScalarFunction("async_func", 3));

        final OneInputTransformation<?, ?> pythonCalc =
                getPythonTransformation(
                        "INSERT INTO sink_table SELECT async_func(a, b), b FROM source_table");

        assertThat(pythonCalc.getParallelism()).isEqualTo(3);
        assertThat(pythonCalc.isParallelismConfigured()).isTrue();
    }

    private OneInputTransformation<?, ?> getPythonTransformation(String statement) {
        final List<Operation> operations = util.getPlanner().getParser().parse(statement);
        final List<Transformation<?>> transformations =
                util.getPlanner()
                        .translate(Collections.singletonList((ModifyOperation) operations.get(0)));
        assertThat(transformations).hasSize(1);
        return findPythonTransformation(transformations.get(0));
    }

    private OneInputTransformation<?, ?> findPythonTransformation(
            Transformation<?> transformation) {
        if (transformation instanceof OneInputTransformation) {
            final OneInputTransformation<?, ?> oneInput =
                    (OneInputTransformation<?, ?>) transformation;
            if (oneInput.getOperatorFactory() instanceof SimpleOperatorFactory) {
                final Object operator =
                        ((SimpleOperatorFactory<?>) oneInput.getOperatorFactory()).getOperator();
                if (operator.getClass().getSimpleName().contains("Python")) {
                    return oneInput;
                }
            }
        }
        for (Transformation<?> input : transformation.getInputs()) {
            final OneInputTransformation<?, ?> result = findPythonTransformation(input);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    private Configuration getPythonOperatorConfig(OneInputTransformation<?, ?> transformation)
            throws Exception {
        final Object operator =
                ((SimpleOperatorFactory<?>) transformation.getOperatorFactory()).getOperator();
        Class<?> operatorClass = operator.getClass();
        while (operatorClass != null) {
            try {
                final Field configField = operatorClass.getDeclaredField("config");
                configField.setAccessible(true);
                return (Configuration) configField.get(operator);
            } catch (NoSuchFieldException ignored) {
                operatorClass = operatorClass.getSuperclass();
            }
        }
        throw new AssertionError("Python operator configuration field was not found.");
    }

    public static class ResourcePandasScalarFunction extends PandasScalarFunction {
        private final int parallelism;
        private final int maxArrowBatchSize;

        public ResourcePandasScalarFunction(String name, int parallelism, int maxArrowBatchSize) {
            super(name);
            this.parallelism = parallelism;
            this.maxArrowBatchSize = maxArrowBatchSize;
        }

        @Override
        public int getParallelism() {
            return parallelism;
        }

        @Override
        public int getMaxArrowBatchSize() {
            return maxArrowBatchSize;
        }
    }

    public static class ResourceAsyncPythonScalarFunction extends AsyncPythonScalarFunction {
        private final int parallelism;

        public ResourceAsyncPythonScalarFunction(String name, int parallelism) {
            super(name);
            this.parallelism = parallelism;
        }

        @Override
        public int getParallelism() {
            return parallelism;
        }
    }
}
