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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Test for {@link DefaultVertexParallelismDecider}. */
public class DefaultVertexParallelismDeciderTest {

    private static final long BYTE_256_MB = 256 * 1024 * 1024L;
    private static final long BYTE_512_MB = 512 * 1024 * 1024L;
    private static final long BYTE_1_GB = 1024 * 1024 * 1024L;
    private static final long BYTE_8_GB = 8 * 1024 * 1024 * 1024L;
    private static final long BYTE_1_TB = 1024 * 1024 * 1024 * 1024L;

    private static final int MAX_PARALLELISM = 100;
    private static final int MIN_PARALLELISM = 3;
    private static final int DEFAULT_SOURCE_PARALLELISM = 10;
    private static final long DATA_VOLUME_PER_TASK = 1024 * 1024 * 1024L;

    @Test
    public void testNormalizedMaxAndMinParallelism() {
        DefaultVertexParallelismDecider decider = createDecider();
        assertThat(decider.getGlobalMaxParallelism(), is(64));
        assertThat(decider.getGlobalMinParallelism(), is(4));
    }

    @Test
    public void testSourceJobVertex() {
        int parallelism = createDeciderAndDecideParallelism(Collections.emptyList());
        assertThat(parallelism, is(DEFAULT_SOURCE_PARALLELISM));
    }

    @Test
    public void testNormalizeParallelismDownToPowerOf2() {
        BlockingResultInfo resultInfo1 =
                BlockingResultInfo.createFromBroadcastResult(Arrays.asList(BYTE_256_MB));
        BlockingResultInfo resultInfo2 =
                BlockingResultInfo.createFromNonBroadcastResult(
                        Arrays.asList(BYTE_256_MB, BYTE_8_GB));

        int parallelism =
                createDeciderAndDecideParallelism(Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism, is(8));
    }

    @Test
    public void testNormalizeParallelismUpToPowerOf2() {
        BlockingResultInfo resultInfo1 =
                BlockingResultInfo.createFromBroadcastResult(Arrays.asList(BYTE_256_MB));
        BlockingResultInfo resultInfo2 =
                BlockingResultInfo.createFromNonBroadcastResult(
                        Arrays.asList(BYTE_1_GB, BYTE_8_GB));

        int parallelism =
                createDeciderAndDecideParallelism(Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism, is(16));
    }

    @Test
    public void testInitiallyNormalizedParallelismIsLargerThanMaxParallelism() {
        BlockingResultInfo resultInfo1 =
                BlockingResultInfo.createFromBroadcastResult(Arrays.asList(BYTE_256_MB));
        BlockingResultInfo resultInfo2 =
                BlockingResultInfo.createFromNonBroadcastResult(
                        Arrays.asList(BYTE_8_GB, BYTE_1_TB));

        int parallelism =
                createDeciderAndDecideParallelism(Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism, is(64));
    }

    @Test
    public void testInitiallyNormalizedParallelismIsSmallerThanMinParallelism() {
        BlockingResultInfo resultInfo1 =
                BlockingResultInfo.createFromBroadcastResult(Arrays.asList(BYTE_256_MB));
        BlockingResultInfo resultInfo2 =
                BlockingResultInfo.createFromNonBroadcastResult(Arrays.asList(BYTE_512_MB));

        int parallelism =
                createDeciderAndDecideParallelism(Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism, is(4));
    }

    @Test
    public void testBroadcastRatioExceedsCapRatio() {
        BlockingResultInfo resultInfo1 =
                BlockingResultInfo.createFromBroadcastResult(Arrays.asList(BYTE_1_GB));
        BlockingResultInfo resultInfo2 =
                BlockingResultInfo.createFromNonBroadcastResult(Arrays.asList(BYTE_8_GB));

        int parallelism =
                createDeciderAndDecideParallelism(Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism, is(16));
    }

    @Test
    public void testNonBroadcastBytesCanNotDividedEvenly() {
        BlockingResultInfo resultInfo1 =
                BlockingResultInfo.createFromBroadcastResult(Arrays.asList(BYTE_512_MB));
        BlockingResultInfo resultInfo2 =
                BlockingResultInfo.createFromNonBroadcastResult(
                        Arrays.asList(BYTE_256_MB, BYTE_8_GB));

        int parallelism =
                createDeciderAndDecideParallelism(Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism, is(16));
    }

    static DefaultVertexParallelismDecider createDecider(
            int minParallelism,
            int maxParallelism,
            long dataVolumePerTask,
            int defaultSourceParallelism) {
        Configuration configuration = new Configuration();
        configuration.setInteger(
                JobManagerOptions.ADAPTIVE_BATCH_SCHEDULER_MIN_PARALLELISM, minParallelism);
        configuration.setInteger(
                JobManagerOptions.ADAPTIVE_BATCH_SCHEDULER_MAX_PARALLELISM, maxParallelism);
        configuration.set(
                JobManagerOptions.ADAPTIVE_BATCH_SCHEDULER_AVG_DATA_VOLUME_PER_TASK,
                new MemorySize(dataVolumePerTask));
        configuration.setInteger(
                JobManagerOptions.ADAPTIVE_BATCH_SCHEDULER_DEFAULT_SOURCE_PARALLELISM,
                defaultSourceParallelism);
        return DefaultVertexParallelismDecider.from(configuration);
    }

    private static DefaultVertexParallelismDecider createDecider() {
        return createDecider(
                MIN_PARALLELISM, MAX_PARALLELISM, DATA_VOLUME_PER_TASK, DEFAULT_SOURCE_PARALLELISM);
    }

    private static int createDeciderAndDecideParallelism(List<BlockingResultInfo> consumedResults) {
        return createDecider()
                .decideParallelismForVertex(new JobVertexID(), consumedResults, MAX_PARALLELISM);
    }
}
