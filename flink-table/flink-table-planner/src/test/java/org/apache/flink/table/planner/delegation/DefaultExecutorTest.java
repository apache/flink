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

package org.apache.flink.table.planner.delegation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.table.delegation.Executor;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertFalse;

/** Test for {@link DefaultExecutor}. */
public class DefaultExecutorTest {

    @Test
    public void testAllVerticesInSameSlotSharingGroupByDefaultIsDisabled() {
        final Executor executor =
                new DefaultExecutor(StreamExecutionEnvironment.getExecutionEnvironment());

        final Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

        final Transformation<?> testTransform =
                new LegacySourceTransformation<>(
                        "MockTransform",
                        new StreamSource<>(
                                new SourceFunction<String>() {
                                    @Override
                                    public void run(SourceContext<String> ctx) {}

                                    @Override
                                    public void cancel() {}
                                }),
                        BasicTypeInfo.STRING_TYPE_INFO,
                        1,
                        Boundedness.BOUNDED);
        final Pipeline pipeline =
                executor.createPipeline(
                        Collections.singletonList(testTransform), configuration, "Test Job");
        final StreamGraph streamGraph = (StreamGraph) pipeline;

        assertFalse(streamGraph.isAllVerticesInSameSlotSharingGroupByDefault());
    }
}
