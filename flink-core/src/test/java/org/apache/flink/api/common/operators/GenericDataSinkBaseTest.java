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

package org.apache.flink.api.common.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.api.common.operators.util.TestIOData;
import org.apache.flink.api.common.operators.util.TestNonRichInputFormat;
import org.apache.flink.api.common.operators.util.TestNonRichOutputFormat;
import org.apache.flink.api.common.operators.util.TestRichOutputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.types.Nothing;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

/** Checks the GenericDataSinkBase operator for both Rich and non-Rich output formats. */
class GenericDataSinkBaseTest implements java.io.Serializable {

    private static final TestNonRichInputFormat in = new TestNonRichInputFormat();
    final GenericDataSourceBase<String, TestNonRichInputFormat> source =
            new GenericDataSourceBase<>(
                    in, new OperatorInformation<>(BasicTypeInfo.STRING_TYPE_INFO), "testSource");

    @Test
    void testDataSourcePlain() throws Exception {
        TestNonRichOutputFormat out = new TestNonRichOutputFormat();
        GenericDataSinkBase<String> sink =
                new GenericDataSinkBase<>(
                        out,
                        new UnaryOperatorInformation<>(
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.getInfoFor(Nothing.class)),
                        "test_sink");
        sink.setInput(source);

        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.disableObjectReuse();
        in.reset();
        sink.executeOnCollections(asList(TestIOData.NAMES), null, executionConfig);
        assertThat(asList(TestIOData.NAMES)).isEqualTo(out.output);

        executionConfig.enableObjectReuse();
        out.clear();
        in.reset();
        sink.executeOnCollections(asList(TestIOData.NAMES), null, executionConfig);
        assertThat(asList(TestIOData.NAMES)).isEqualTo(out.output);
    }

    @Test
    void testDataSourceWithRuntimeContext() throws Exception {
        TestRichOutputFormat out = new TestRichOutputFormat();
        GenericDataSinkBase<String> sink =
                new GenericDataSinkBase<>(
                        out,
                        new UnaryOperatorInformation<>(
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.getInfoFor(Nothing.class)),
                        "test_sink");
        sink.setInput(source);

        ExecutionConfig executionConfig = new ExecutionConfig();
        final HashMap<String, Accumulator<?, ?>> accumulatorMap = new HashMap<>();
        final HashMap<String, Future<Path>> cpTasks = new HashMap<>();
        final TaskInfo taskInfo = new TaskInfoImpl("test_sink", 1, 0, 1, 0);
        executionConfig.disableObjectReuse();
        in.reset();

        sink.executeOnCollections(
                asList(TestIOData.NAMES),
                new RuntimeUDFContext(
                        taskInfo,
                        null,
                        executionConfig,
                        cpTasks,
                        accumulatorMap,
                        UnregisteredMetricsGroup.createOperatorMetricGroup()),
                executionConfig);

        assertThat(asList(TestIOData.RICH_NAMES)).isEqualTo(out.output);

        executionConfig.enableObjectReuse();
        out.clear();
        in.reset();

        sink.executeOnCollections(
                asList(TestIOData.NAMES),
                new RuntimeUDFContext(
                        taskInfo,
                        null,
                        executionConfig,
                        cpTasks,
                        accumulatorMap,
                        UnregisteredMetricsGroup.createOperatorMetricGroup()),
                executionConfig);
        assertThat(asList(TestIOData.RICH_NAMES)).isEqualTo(out.output);
    }
}
