/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.api;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.functions.BroadcastStateBootstrapFunction;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.state.api.functions.StateBootstrapFunction;
import org.apache.flink.state.api.output.TaggedOperatorSubtaskState;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Assert;
import org.junit.Test;

/** Tests for bootstrap transformations. */
public class StateBootstrapTransformationTest extends AbstractTestBase {

    @Test
    public void testBroadcastStateTransformationParallelism() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);

        DataStream<Integer> input = env.fromElements(0);

        StateBootstrapTransformation<Integer> transformation =
                OperatorTransformation.bootstrapWith(input)
                        .transform(new ExampleBroadcastStateBootstrapFunction());

        int maxParallelism = transformation.getMaxParallelism(4);
        DataStream<TaggedOperatorSubtaskState> result =
                transformation.writeOperatorSubtaskStates(
                        OperatorIDGenerator.fromUid("uid"),
                        new HashMapStateBackend(),
                        new Path(),
                        maxParallelism);

        Assert.assertEquals(
                "Broadcast transformations should always be run at parallelism 1",
                1,
                result.getParallelism());
    }

    @Test
    public void testDefaultParallelismRespectedWhenLessThanMaxParallelism() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<Integer> input = env.fromElements(0);

        StateBootstrapTransformation<Integer> transformation =
                OperatorTransformation.bootstrapWith(input)
                        .transform(new ExampleStateBootstrapFunction());

        int maxParallelism = transformation.getMaxParallelism(10);
        DataStream<TaggedOperatorSubtaskState> result =
                transformation.writeOperatorSubtaskStates(
                        OperatorIDGenerator.fromUid("uid"),
                        new HashMapStateBackend(),
                        new Path(),
                        maxParallelism);

        Assert.assertEquals(
                "The parallelism of a data set should not change when less than the max parallelism of the savepoint",
                env.getParallelism(),
                result.getParallelism());
    }

    @Test
    public void testMaxParallelismRespected() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);

        DataStream<Integer> input = env.fromElements(0);

        StateBootstrapTransformation<Integer> transformation =
                OperatorTransformation.bootstrapWith(input)
                        .transform(new ExampleStateBootstrapFunction());

        int maxParallelism = transformation.getMaxParallelism(4);
        DataStream<TaggedOperatorSubtaskState> result =
                transformation.writeOperatorSubtaskStates(
                        OperatorIDGenerator.fromUid("uid"),
                        new HashMapStateBackend(),
                        new Path(),
                        maxParallelism);

        Assert.assertEquals(
                "The parallelism of a data set should be constrained my the savepoint max parallelism",
                4,
                result.getParallelism());
    }

    @Test
    public void testOperatorSpecificMaxParallelismRespected() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<Integer> input = env.fromElements(0);

        StateBootstrapTransformation<Integer> transformation =
                OperatorTransformation.bootstrapWith(input)
                        .setMaxParallelism(1)
                        .transform(new ExampleStateBootstrapFunction());

        int maxParallelism = transformation.getMaxParallelism(4);
        DataStream<TaggedOperatorSubtaskState> result =
                transformation.writeOperatorSubtaskStates(
                        OperatorIDGenerator.fromUid("uid"),
                        new HashMapStateBackend(),
                        new Path(),
                        maxParallelism);

        Assert.assertEquals(
                "The parallelism of a data set should be constrained my the savepoint max parallelism",
                1,
                result.getParallelism());
    }

    @Test
    public void testStreamConfig() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = env.fromElements("");

        StateBootstrapTransformation<String> transformation =
                OperatorTransformation.bootstrapWith(input)
                        .keyBy(new CustomKeySelector())
                        .transform(new ExampleKeyedStateBootstrapFunction());

        StreamConfig config =
                transformation.getConfig(
                        OperatorIDGenerator.fromUid("uid"),
                        new HashMapStateBackend(),
                        new Configuration(),
                        null);
        KeySelector selector =
                config.getStatePartitioner(0, Thread.currentThread().getContextClassLoader());

        Assert.assertEquals(
                "Incorrect key selector forwarded to stream operator",
                CustomKeySelector.class,
                selector.getClass());
    }

    private static class CustomKeySelector implements KeySelector<String, String> {

        @Override
        public String getKey(String value) throws Exception {
            return value;
        }
    }

    private static class ExampleBroadcastStateBootstrapFunction
            extends BroadcastStateBootstrapFunction<Integer> {

        @Override
        public void processElement(Integer value, Context ctx) throws Exception {}
    }

    private static class ExampleStateBootstrapFunction extends StateBootstrapFunction<Integer> {

        @Override
        public void processElement(Integer value, Context ctx) throws Exception {}

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {}

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {}
    }

    private static class ExampleKeyedStateBootstrapFunction
            extends KeyedStateBootstrapFunction<String, String> {

        @Override
        public void processElement(String value, Context ctx) throws Exception {}
    }
}
