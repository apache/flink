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

package org.apache.flink.connector.file.sink;

import org.apache.flink.connector.file.sink.utils.IntegerFileSinkTestDataUtils;
import org.apache.flink.connector.file.sink.utils.PartSizeAndCheckpointRollingPolicy;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

/** The base class for the File Sink IT Case in different execution mode. */
abstract class FileSinkITBase {

    protected static final int NUM_SOURCES = 4;

    protected static final int NUM_SINKS = 3;

    protected static final int NUM_RECORDS = 10000;

    protected static final int NUM_BUCKETS = 4;

    protected static final double FAILOVER_RATIO = 0.4;

    private static Stream<Boolean> params() {
        return Stream.of(false, true);
    }

    @ParameterizedTest(name = "triggerFailover = {0}")
    @MethodSource("params")
    void testFileSink(boolean triggerFailover, @TempDir java.nio.file.Path tmpDir)
            throws Exception {
        String path = tmpDir.toString();

        JobGraph jobGraph = createJobGraph(triggerFailover, path);

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .withRandomPorts()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(4)
                        .build();

        try (MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        }

        IntegerFileSinkTestDataUtils.checkIntegerSequenceSinkOutput(
                path, NUM_RECORDS, NUM_BUCKETS, NUM_SOURCES);
    }

    protected abstract JobGraph createJobGraph(boolean triggerFailover, String path);

    protected FileSink<Integer> createFileSink(String path) {
        return FileSink.forRowFormat(new Path(path), new IntegerFileSinkTestDataUtils.IntEncoder())
                .withBucketAssigner(
                        new IntegerFileSinkTestDataUtils.ModuloBucketAssigner(NUM_BUCKETS))
                .withRollingPolicy(new PartSizeAndCheckpointRollingPolicy<>(1024, true))
                .build();
    }
}
