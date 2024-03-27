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

package org.apache.flink.api.common.operators.base;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The test for flat map operator. */
public class FlatMapOperatorCollectionTest implements Serializable {

    @Test
    void testExecuteOnCollection() throws Exception {
        IdRichFlatMap<String> udf = new IdRichFlatMap<>();
        testExecuteOnCollection(udf, Arrays.asList("f", "l", "i", "n", "k"), true);
        assertThat(udf.isClosed).isTrue();

        udf = new IdRichFlatMap<>();
        testExecuteOnCollection(udf, Arrays.asList("f", "l", "i", "n", "k"), false);
        assertThat(udf.isClosed).isTrue();

        udf = new IdRichFlatMap<>();
        testExecuteOnCollection(udf, Collections.emptyList(), true);
        assertThat(udf.isClosed).isTrue();

        udf = new IdRichFlatMap<>();
        testExecuteOnCollection(udf, Collections.emptyList(), false);
        assertThat(udf.isClosed).isTrue();
    }

    private void testExecuteOnCollection(
            FlatMapFunction<String, String> udf, List<String> input, boolean mutableSafe)
            throws Exception {
        ExecutionConfig executionConfig = new ExecutionConfig();
        if (mutableSafe) {
            executionConfig.disableObjectReuse();
        } else {
            executionConfig.enableObjectReuse();
        }
        final TaskInfo taskInfo = new TaskInfoImpl("Test UDF", 4, 0, 4, 0);
        // run on collections
        final List<String> result =
                getTestFlatMapOperator(udf)
                        .executeOnCollections(
                                input,
                                new RuntimeUDFContext(
                                        taskInfo,
                                        null,
                                        executionConfig,
                                        new HashMap<>(),
                                        new HashMap<>(),
                                        UnregisteredMetricsGroup.createOperatorMetricGroup()),
                                executionConfig);

        assertThat(result).hasSameSizeAs(input);
        assertThat(result).isEqualTo(input);
    }

    /** The test flat map function. */
    public class IdRichFlatMap<IN> extends RichFlatMapFunction<IN, IN> {

        private boolean isOpened = false;
        private boolean isClosed = false;

        @Override
        public void open(OpenContext openContext) {
            isOpened = true;

            RuntimeContext ctx = getRuntimeContext();
            assertThat(ctx.getTaskInfo().getTaskName()).isEqualTo("Test UDF");
            assertThat(ctx.getTaskInfo().getNumberOfParallelSubtasks()).isEqualTo(4);
            assertThat(ctx.getTaskInfo().getIndexOfThisSubtask()).isZero();
        }

        @Override
        public void flatMap(IN value, Collector<IN> out) {
            assertThat(isOpened).isTrue();
            assertThat(isClosed).isFalse();

            out.collect(value);
        }

        @Override
        public void close() {
            isClosed = true;
        }
    }

    private FlatMapOperatorBase<String, String, FlatMapFunction<String, String>>
            getTestFlatMapOperator(FlatMapFunction<String, String> udf) {

        UnaryOperatorInformation<String, String> typeInfo =
                new UnaryOperatorInformation<>(
                        BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        return new FlatMapOperatorBase<>(udf, typeInfo, "flatMap on Collections");
    }
}
