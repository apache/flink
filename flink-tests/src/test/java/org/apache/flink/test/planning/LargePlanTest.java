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

package org.apache.flink.test.planning;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.Test;

/** Tests that large programs can be compiled to a Plan in reasonable amount of time. */
public class LargePlanTest {

    @Test(timeout = 30_000)
    public void testPlanningOfLargePlan() throws Exception {
        runProgram(10, 20);
    }

    private static void runProgram(int depth, int width) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> input = env.fromElements("a", "b", "c");
        DataSet<String> stats = null;

        for (int i = 0; i < depth; i++) {
            stats = analyze(input, stats, width / (i + 1) + 1);
        }

        stats.output(new DiscardingOutputFormat<>());

        env.createProgramPlan("depth " + depth + " width " + width);
    }

    private static DataSet<String> analyze(
            DataSet<String> input, DataSet<String> stats, int branches) {
        for (int i = 0; i < branches; i++) {
            final int ii = i;

            if (stats != null) {
                input =
                        input.map(
                                        new RichMapFunction<String, String>() {
                                            @Override
                                            public String map(String value) {
                                                return value;
                                            }
                                        })
                                .withBroadcastSet(stats.map(s -> "(" + s + ").map"), "stats");
            }

            DataSet<String> branch =
                    input.map(s -> new Tuple2<>(0, s + ii))
                            .returns(Types.TUPLE(Types.STRING, Types.INT))
                            .groupBy(0)
                            .minBy(1)
                            .map(kv -> kv.f1)
                            .returns(Types.STRING);
            if (stats == null) {
                stats = branch;
            } else {
                stats = stats.union(branch);
            }
        }
        return stats.map(s -> "(" + s + ").stats");
    }
}
