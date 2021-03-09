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

package org.apache.flink.api.java.utils;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.Test;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/** Tests for {@link PlanGenerator}. */
public class PlanGeneratorTest {

    @Test
    public void testGenerate() {

        final String fileA = "fileA";
        final String fileB = "fileB";

        final Map<String, DistributedCache.DistributedCacheEntry> originalArtifacts =
                Stream.of(
                                Tuple2.of(
                                        fileA,
                                        new DistributedCache.DistributedCacheEntry("test1", true)),
                                Tuple2.of(
                                        fileB,
                                        new DistributedCache.DistributedCacheEntry("test2", false)))
                        .collect(Collectors.toMap(x -> x.f0, x -> x.f1));

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);
        env.registerCachedFile("test1", fileA, true);
        env.registerCachedFile("test2", fileB, false);

        env.fromElements(1, 3, 5)
                .map((MapFunction<Integer, String>) value -> String.valueOf(value + 1))
                .writeAsText("/tmp/csv");

        final Plan generatedPlanUnderTest = env.createProgramPlan("test");

        final Map<String, DistributedCache.DistributedCacheEntry> retrievedArtifacts =
                generatedPlanUnderTest.getCachedFiles().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        assertEquals(1, generatedPlanUnderTest.getDataSinks().size());
        assertEquals(10, generatedPlanUnderTest.getDefaultParallelism());
        assertEquals(env.getConfig(), generatedPlanUnderTest.getExecutionConfig());
        assertEquals("test", generatedPlanUnderTest.getJobName());

        assertEquals(originalArtifacts.size(), retrievedArtifacts.size());
        assertEquals(originalArtifacts.get(fileA), retrievedArtifacts.get(fileA));
        assertEquals(originalArtifacts.get(fileB), retrievedArtifacts.get(fileB));
    }
}
