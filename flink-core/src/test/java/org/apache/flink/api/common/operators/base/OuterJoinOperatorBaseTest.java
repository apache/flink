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
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.Collector;

import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("serial")
public class OuterJoinOperatorBaseTest implements Serializable {

    private MockRichFlatJoinFunction joiner;

    private OuterJoinOperatorBase<String, String, String, FlatJoinFunction<String, String, String>>
            baseOperator;

    private ExecutionConfig executionConfig;

    private RuntimeContext runtimeContext;

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Before
    public void setup() {
        joiner = new MockRichFlatJoinFunction();

        baseOperator =
                new OuterJoinOperatorBase(
                        joiner,
                        new BinaryOperatorInformation(
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO),
                        new int[0],
                        new int[0],
                        "TestJoiner",
                        null);

        executionConfig = new ExecutionConfig();

        String taskName = "Test rich outer join function";
        TaskInfo taskInfo = new TaskInfo(taskName, 1, 0, 1, 0);
        HashMap<String, Accumulator<?, ?>> accumulatorMap = new HashMap<>();
        HashMap<String, Future<Path>> cpTasks = new HashMap<>();

        runtimeContext =
                new RuntimeUDFContext(
                        taskInfo,
                        null,
                        executionConfig,
                        cpTasks,
                        accumulatorMap,
                        UnregisteredMetricsGroup.createOperatorMetricGroup());
    }

    @Test
    public void testFullOuterJoinWithoutMatchingPartners() throws Exception {
        final List<String> leftInput = Arrays.asList("foo", "bar", "foobar");
        final List<String> rightInput = Arrays.asList("oof", "rab", "raboof");
        baseOperator.setOuterJoinType(OuterJoinOperatorBase.OuterJoinType.FULL);
        List<String> expected =
                Arrays.asList(
                        "bar,null",
                        "foo,null",
                        "foobar,null",
                        "null,oof",
                        "null,rab",
                        "null,raboof");
        testOuterJoin(leftInput, rightInput, expected);
    }

    @Test
    public void testFullOuterJoinWithFullMatchingKeys() throws Exception {
        final List<String> leftInput = Arrays.asList("foo", "bar", "foobar");
        final List<String> rightInput = Arrays.asList("bar", "foobar", "foo");
        baseOperator.setOuterJoinType(OuterJoinOperatorBase.OuterJoinType.FULL);
        List<String> expected = Arrays.asList("bar,bar", "foo,foo", "foobar,foobar");
        testOuterJoin(leftInput, rightInput, expected);
    }

    @Test
    public void testFullOuterJoinWithEmptyLeftInput() throws Exception {
        final List<String> leftInput = Collections.emptyList();
        final List<String> rightInput = Arrays.asList("foo", "bar", "foobar");
        baseOperator.setOuterJoinType(OuterJoinOperatorBase.OuterJoinType.FULL);
        List<String> expected = Arrays.asList("null,bar", "null,foo", "null,foobar");
        testOuterJoin(leftInput, rightInput, expected);
    }

    @Test
    public void testFullOuterJoinWithEmptyRightInput() throws Exception {
        final List<String> leftInput = Arrays.asList("foo", "bar", "foobar");
        final List<String> rightInput = Collections.emptyList();
        baseOperator.setOuterJoinType(OuterJoinOperatorBase.OuterJoinType.FULL);
        List<String> expected = Arrays.asList("bar,null", "foo,null", "foobar,null");
        testOuterJoin(leftInput, rightInput, expected);
    }

    @Test
    public void testFullOuterJoinWithPartialMatchingKeys() throws Exception {
        final List<String> leftInput = Arrays.asList("foo", "bar", "foobar");
        final List<String> rightInput = Arrays.asList("bar", "foo", "barfoo");
        baseOperator.setOuterJoinType(OuterJoinOperatorBase.OuterJoinType.FULL);
        List<String> expected = Arrays.asList("bar,bar", "null,barfoo", "foo,foo", "foobar,null");
        testOuterJoin(leftInput, rightInput, expected);
    }

    @Test
    public void testFullOuterJoinBuildingCorrectCrossProducts() throws Exception {
        final List<String> leftInput =
                Arrays.asList("foo", "foo", "foo", "bar", "bar", "foobar", "foobar");
        final List<String> rightInput =
                Arrays.asList("foo", "foo", "bar", "bar", "bar", "barfoo", "barfoo");
        baseOperator.setOuterJoinType(OuterJoinOperatorBase.OuterJoinType.FULL);
        List<String> expected =
                Arrays.asList(
                        "bar,bar",
                        "bar,bar",
                        "bar,bar",
                        "bar,bar",
                        "bar,bar",
                        "bar,bar",
                        "null,barfoo",
                        "null,barfoo",
                        "foo,foo",
                        "foo,foo",
                        "foo,foo",
                        "foo,foo",
                        "foo,foo",
                        "foo,foo",
                        "foobar,null",
                        "foobar,null");
        testOuterJoin(leftInput, rightInput, expected);
    }

    @Test
    public void testLeftOuterJoin() throws Exception {
        final List<String> leftInput =
                Arrays.asList("foo", "foo", "foo", "bar", "bar", "foobar", "foobar");
        final List<String> rightInput =
                Arrays.asList("foo", "foo", "bar", "bar", "bar", "barfoo", "barfoo");
        baseOperator.setOuterJoinType(OuterJoinOperatorBase.OuterJoinType.LEFT);
        List<String> expected =
                Arrays.asList(
                        "bar,bar",
                        "bar,bar",
                        "bar,bar",
                        "bar,bar",
                        "bar,bar",
                        "bar,bar",
                        "foo,foo",
                        "foo,foo",
                        "foo,foo",
                        "foo,foo",
                        "foo,foo",
                        "foo,foo",
                        "foobar,null",
                        "foobar,null");
        testOuterJoin(leftInput, rightInput, expected);
    }

    @Test
    public void testRightOuterJoin() throws Exception {
        final List<String> leftInput =
                Arrays.asList("foo", "foo", "foo", "bar", "bar", "foobar", "foobar");
        final List<String> rightInput =
                Arrays.asList("foo", "foo", "bar", "bar", "bar", "barfoo", "barfoo");
        baseOperator.setOuterJoinType(OuterJoinOperatorBase.OuterJoinType.RIGHT);
        List<String> expected =
                Arrays.asList(
                        "bar,bar",
                        "bar,bar",
                        "bar,bar",
                        "bar,bar",
                        "bar,bar",
                        "bar,bar",
                        "null,barfoo",
                        "null,barfoo",
                        "foo,foo",
                        "foo,foo",
                        "foo,foo",
                        "foo,foo",
                        "foo,foo",
                        "foo,foo");
        testOuterJoin(leftInput, rightInput, expected);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThatExceptionIsThrownForOuterJoinTypeNull() throws Exception {
        final List<String> leftInput = Arrays.asList("foo", "bar", "foobar");
        final List<String> rightInput = Arrays.asList("bar", "foobar", "foo");

        baseOperator.setOuterJoinType(null);
        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.disableObjectReuse();
        baseOperator.executeOnCollections(leftInput, rightInput, runtimeContext, executionConfig);
    }

    private void testOuterJoin(
            List<String> leftInput, List<String> rightInput, List<String> expected)
            throws Exception {
        executionConfig.disableObjectReuse();
        List<String> resultSafe =
                baseOperator.executeOnCollections(
                        leftInput, rightInput, runtimeContext, executionConfig);
        executionConfig.enableObjectReuse();
        List<String> resultRegular =
                baseOperator.executeOnCollections(
                        leftInput, rightInput, runtimeContext, executionConfig);

        assertEquals(expected, resultSafe);
        assertEquals(expected, resultRegular);

        assertTrue(joiner.opened.get());
        assertTrue(joiner.closed.get());
    }

    private static class MockRichFlatJoinFunction
            extends RichFlatJoinFunction<String, String, String> {
        final AtomicBoolean opened = new AtomicBoolean(false);
        final AtomicBoolean closed = new AtomicBoolean(false);

        @Override
        public void open(OpenContext openContext) throws Exception {
            opened.compareAndSet(false, true);
            assertEquals(0, getRuntimeContext().getIndexOfThisSubtask());
            assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());
        }

        @Override
        public void close() throws Exception {
            closed.compareAndSet(false, true);
        }

        @Override
        public void join(String first, String second, Collector<String> out) throws Exception {
            out.collect(String.valueOf(first) + ',' + String.valueOf(second));
        }
    }
}
