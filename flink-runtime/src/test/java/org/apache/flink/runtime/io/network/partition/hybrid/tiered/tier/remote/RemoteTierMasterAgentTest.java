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

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile.getPartitionPath;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RemoteTierMasterAgent}. */
class RemoteTierMasterAgentTest {

    @TempDir private File tempFolder;

    @Test
    void testAddAndReleasePartition() throws IOException {
        TieredStoragePartitionId partitionId =
                TieredStorageIdMappingUtils.convertId(new ResultPartitionID());
        File partitionFile = new File(getPartitionPath(partitionId, tempFolder.getAbsolutePath()));
        assertThat(partitionFile.createNewFile()).isTrue();
        assertThat(partitionFile.exists()).isTrue();

        TieredStorageResourceRegistry resourceRegistry = new TieredStorageResourceRegistry();
        RemoteTierMasterAgent masterAgent =
                new RemoteTierMasterAgent(tempFolder.getAbsolutePath(), resourceRegistry);
        masterAgent.addPartition(partitionId);
        assertThat(partitionFile.exists()).isTrue();
        masterAgent.releasePartition(partitionId);

        assertThat(partitionFile.exists()).isFalse();
    }
}
