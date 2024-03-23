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

package org.apache.flink.streaming.pathtracker;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.streaming.api.environment.PathAnalyzer;
import org.apache.flink.streaming.api.environment.PathTrackerOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import org.junit.Test;

import java.util.Collections;

public class PathAnalyzerTest {
    private static final String EXEC_NAME = "test-executor";

    @Test
    public void pathComputationTest() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(DeploymentOptions.TARGET, EXEC_NAME);
        configuration.set(PathTrackerOptions.ENABLE, true);

        final StreamExecutionEnvironment env = new StreamExecutionEnvironment(configuration);
        env.fromData(Collections.singletonList(123))
                .setParallelism(1)
                .map(
                        (x) -> {
                            return 2 * x;
                        })
                .setParallelism(3)
                .rebalance()
                .map(
                        (x) -> {
                            return x * x;
                        })
                .setParallelism(4)
                .addSink(new DiscardingSink<>());

        int pathNum = PathAnalyzer.computePathNum(env);
        assert (pathNum == 12);
    }
}
