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
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.api.common.operators.util.TestIOData;
import org.apache.flink.api.common.operators.util.TestNonRichInputFormat;
import org.apache.flink.api.common.operators.util.TestRichInputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Checks the GenericDataSourceBase operator for both Rich and non-Rich input formats. */
@SuppressWarnings("serial")
public class GenericDataSourceBaseTest implements java.io.Serializable {

    @Test
    public void testDataSourcePlain() {
        try {
            TestNonRichInputFormat in = new TestNonRichInputFormat();
            GenericDataSourceBase<String, TestNonRichInputFormat> source =
                    new GenericDataSourceBase<String, TestNonRichInputFormat>(
                            in,
                            new OperatorInformation<String>(BasicTypeInfo.STRING_TYPE_INFO),
                            "testSource");

            ExecutionConfig executionConfig = new ExecutionConfig();
            executionConfig.disableObjectReuse();
            List<String> resultMutableSafe = source.executeOnCollections(null, executionConfig);

            in.reset();
            executionConfig.enableObjectReuse();
            List<String> resultRegular = source.executeOnCollections(null, executionConfig);
            assertEquals(asList(TestIOData.NAMES), resultMutableSafe);
            assertEquals(asList(TestIOData.NAMES), resultRegular);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testDataSourceWithRuntimeContext() {
        try {
            TestRichInputFormat in = new TestRichInputFormat();
            GenericDataSourceBase<String, TestRichInputFormat> source =
                    new GenericDataSourceBase<String, TestRichInputFormat>(
                            in,
                            new OperatorInformation<String>(BasicTypeInfo.STRING_TYPE_INFO),
                            "testSource");

            final HashMap<String, Accumulator<?, ?>> accumulatorMap =
                    new HashMap<String, Accumulator<?, ?>>();
            final HashMap<String, Future<Path>> cpTasks = new HashMap<>();
            final TaskInfo taskInfo = new TaskInfo("test_source", 1, 0, 1, 0);

            ExecutionConfig executionConfig = new ExecutionConfig();
            executionConfig.disableObjectReuse();
            assertEquals(false, in.hasBeenClosed());
            assertEquals(false, in.hasBeenOpened());

            List<String> resultMutableSafe =
                    source.executeOnCollections(
                            new RuntimeUDFContext(
                                    taskInfo,
                                    null,
                                    executionConfig,
                                    cpTasks,
                                    accumulatorMap,
                                    new UnregisteredMetricsGroup()),
                            executionConfig);

            assertEquals(true, in.hasBeenClosed());
            assertEquals(true, in.hasBeenOpened());

            in.reset();
            executionConfig.enableObjectReuse();
            assertEquals(false, in.hasBeenClosed());
            assertEquals(false, in.hasBeenOpened());

            List<String> resultRegular =
                    source.executeOnCollections(
                            new RuntimeUDFContext(
                                    taskInfo,
                                    null,
                                    executionConfig,
                                    cpTasks,
                                    accumulatorMap,
                                    new UnregisteredMetricsGroup()),
                            executionConfig);

            assertEquals(true, in.hasBeenClosed());
            assertEquals(true, in.hasBeenOpened());

            assertEquals(asList(TestIOData.RICH_NAMES), resultMutableSafe);
            assertEquals(asList(TestIOData.RICH_NAMES), resultRegular);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
