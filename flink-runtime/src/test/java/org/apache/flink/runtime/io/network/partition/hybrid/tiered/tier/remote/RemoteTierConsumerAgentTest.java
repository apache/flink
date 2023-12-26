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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.TestingPartitionFileReader;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RemoteTierConsumerAgent}. */
class RemoteTierConsumerAgentTest {

    @TempDir private File tempFolder;

    private String remoteStoragePath;

    @BeforeEach
    void before() {
        remoteStoragePath = Path.fromLocalFile(tempFolder).getPath();
    }

    @Test
    void testGetEmptyBuffer() {
        RemoteTierConsumerAgent remoteTierConsumerAgent =
                new RemoteTierConsumerAgent(
                        new RemoteStorageScanner(remoteStoragePath),
                        new TestingPartitionFileReader.Builder().build(),
                        1024);
        assertThat(
                        remoteTierConsumerAgent.getNextBuffer(
                                new TieredStoragePartitionId(new ResultPartitionID()),
                                new TieredStorageSubpartitionId(0),
                                0))
                .isEmpty();
    }

    @Test
    void testGetBuffer() {
        int bufferSize = 10;
        PartitionFileReader partitionFileReader =
                new TestingPartitionFileReader.Builder()
                        .setReadBufferSupplier(
                                (bufferIndex, segmentId) ->
                                        new PartitionFileReader.ReadBufferResult(
                                                Collections.singletonList(
                                                        BufferBuilderTestUtils.buildSomeBuffer(
                                                                bufferSize)),
                                                false,
                                                null))
                        .build();
        RemoteTierConsumerAgent remoteTierConsumerAgent =
                new RemoteTierConsumerAgent(
                        new RemoteStorageScanner(remoteStoragePath), partitionFileReader, 1024);
        Optional<Buffer> optionalBuffer =
                remoteTierConsumerAgent.getNextBuffer(
                        new TieredStoragePartitionId(new ResultPartitionID()),
                        new TieredStorageSubpartitionId(0),
                        0);
        assertThat(optionalBuffer)
                .hasValueSatisfying(
                        buffer -> assertThat(buffer.readableBytes()).isEqualTo(bufferSize));
    }
}
