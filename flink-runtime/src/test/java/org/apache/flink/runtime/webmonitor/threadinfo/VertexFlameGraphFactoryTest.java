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

package org.apache.flink.runtime.webmonitor.threadinfo;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraphID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.ThreadInfoSample;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link VertexFlameGraphFactory}. */
class VertexFlameGraphFactoryTest {
    /** Tests that lambda class names are cleaned up inside the stack traces. */
    @Test
    void testLambdaClassNamesCleanUp() {
        Map<ExecutionAttemptID, Collection<ThreadInfoSample>> samplesBySubtask = generateSamples();

        VertexThreadInfoStats sample = new VertexThreadInfoStats(0, 0, 0, samplesBySubtask);

        VertexFlameGraph graph = VertexFlameGraphFactory.createFullFlameGraphFrom(sample);
        int encounteredLambdas = verifyRecursively(graph.getRoot());
        if (encounteredLambdas == 0) {
            fail("No lambdas encountered in the test, cleanup functionality was not tested");
        }
    }

    private int verifyRecursively(VertexFlameGraph.Node node) {
        String location = node.getStackTraceLocation();
        int lambdas = 0;
        final String javaVersion = System.getProperty("java.version");
        if (javaVersion.compareTo("21") < 0 && location.contains("$Lambda$")
                || javaVersion.compareTo("21") >= 0 && location.contains("$$Lambda")) {
            lambdas++;
            //    com.example.ClassName.method:123
            // -> com.example.ClassName.method
            // -> com.example.ClassName
            String locationWithoutLineNumber = location.substring(0, location.lastIndexOf(":"));
            String className =
                    locationWithoutLineNumber.substring(
                            0, locationWithoutLineNumber.lastIndexOf("."));
            assertThat(className)
                    .is(
                            new Condition<String>() {
                                @Override
                                public boolean matches(String value) {

                                    return javaVersion.startsWith("1.8")
                                                    && value.endsWith("$Lambda$0/0")
                                            || javaVersion.compareTo("21") < 0
                                                    && value.endsWith("$Lambda$0/0x0")
                                            || value.endsWith("$$Lambda0/0x0");
                                }
                            });
        }
        return lambdas + node.getChildren().stream().mapToInt(this::verifyRecursively).sum();
    }

    private Map<ExecutionAttemptID, Collection<ThreadInfoSample>> generateSamples() {
        ThreadInfoSample sample1 = ThreadInfoSample.from(getStackTraceWithLambda()).get();

        List<ThreadInfoSample> samples = new ArrayList<>();
        samples.add(sample1);

        ExecutionAttemptID executionAttemptID =
                new ExecutionAttemptID(
                        new ExecutionGraphID(), new ExecutionVertexID(new JobVertexID(), 0), 0);

        Map<ExecutionAttemptID, Collection<ThreadInfoSample>> result = new HashMap<>();
        result.put(executionAttemptID, samples);

        return result;
    }

    private ThreadInfo getStackTraceWithLambda() {
        Supplier<ThreadInfo> r1 =
                () ->
                        ManagementFactory.getThreadMXBean()
                                .getThreadInfo(Thread.currentThread().getId(), Integer.MAX_VALUE);
        Supplier<ThreadInfo> r2 = () -> r1.get();
        return r2.get();
    }
}
