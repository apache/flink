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

import org.apache.flink.connector.file.sink.compactor.DecoderBasedReader;
import org.apache.flink.connector.file.sink.compactor.FileCompactStrategy;
import org.apache.flink.connector.file.sink.compactor.FileCompactor;
import org.apache.flink.connector.file.sink.compactor.RecordWiseFileCompactor;
import org.apache.flink.connector.file.sink.utils.IntegerFileSinkTestDataUtils;
import org.apache.flink.connector.file.sink.utils.IntegerFileSinkTestDataUtils.IntDecoder;
import org.apache.flink.connector.file.sink.utils.PartSizeAndCheckpointRollingPolicy;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Tests the compaction of the {@link FileSink} in STREAMING mode. */
@RunWith(Parameterized.class)
public class StreamingCompactingFileSinkITCase extends StreamingExecutionFileSinkITCase {

    private static final int PARALLELISM = 4;

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    @Override
    protected FileSink<Integer> createFileSink(String path) {
        return FileSink.forRowFormat(new Path(path), new IntegerFileSinkTestDataUtils.IntEncoder())
                .withBucketAssigner(
                        new IntegerFileSinkTestDataUtils.ModuloBucketAssigner(NUM_BUCKETS))
                .withRollingPolicy(new PartSizeAndCheckpointRollingPolicy<>(1024, false))
                .enableCompact(createFileCompactStrategy(), createFileCompactor())
                .build();
    }

    @Override
    protected void configureSink(DataStreamSink<Integer> sink) {
        sink.uid("sink");
    }

    private static FileCompactor createFileCompactor() {
        return new RecordWiseFileCompactor<>(new DecoderBasedReader.Factory<>(IntDecoder::new));
    }

    private static FileCompactStrategy createFileCompactStrategy() {
        return FileCompactStrategy.Builder.newBuilder().setSizeThreshold(10000).build();
    }
}
