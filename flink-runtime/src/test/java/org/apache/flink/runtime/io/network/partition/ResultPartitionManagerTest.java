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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.apache.flink.runtime.io.network.partition.PartitionTestUtils.createPartition;
import static org.apache.flink.runtime.io.network.partition.PartitionTestUtils.verifyCreateSubpartitionViewThrowsException;

/** Tests for {@link ResultPartitionManager}. */
public class ResultPartitionManagerTest extends TestLogger {

    /**
     * Tests that {@link ResultPartitionManager#createSubpartitionView(ResultPartitionID, int,
     * BufferAvailabilityListener)} would throw {@link PartitionNotFoundException} if this partition
     * was not registered before.
     */
    @Test
    public void testThrowPartitionNotFoundException() throws Exception {
        final ResultPartitionManager partitionManager = new ResultPartitionManager();
        final ResultPartition partition = createPartition();

        verifyCreateSubpartitionViewThrowsException(partitionManager, partition.getPartitionId());
    }

    /**
     * Tests {@link ResultPartitionManager#createSubpartitionView(ResultPartitionID, int,
     * BufferAvailabilityListener)} successful if this partition was already registered before.
     */
    @Test
    public void testCreateViewForRegisteredPartition() throws Exception {
        final ResultPartitionManager partitionManager = new ResultPartitionManager();
        final ResultPartition partition = createPartition();

        partitionManager.registerResultPartition(partition);
        partitionManager.createSubpartitionView(
                partition.getPartitionId(), 0, new NoOpBufferAvailablityListener());
    }

    /**
     * Tests {@link ResultPartitionManager#createSubpartitionView(ResultPartitionID, int,
     * BufferAvailabilityListener)} would throw a {@link PartitionNotFoundException} if this
     * partition was already released before.
     */
    @Test
    public void testCreateViewForReleasedPartition() throws Exception {
        final ResultPartitionManager partitionManager = new ResultPartitionManager();
        final ResultPartition partition = createPartition();

        partitionManager.registerResultPartition(partition);
        partitionManager.releasePartition(partition.getPartitionId(), null);

        verifyCreateSubpartitionViewThrowsException(partitionManager, partition.getPartitionId());
    }
}
