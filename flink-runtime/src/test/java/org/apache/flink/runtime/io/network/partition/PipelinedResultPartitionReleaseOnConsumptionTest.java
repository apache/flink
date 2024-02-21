/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.UnpooledBufferPool;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link PipelinedResultPartitionReleaseOnConsumptionTest}. */
class PipelinedResultPartitionReleaseOnConsumptionTest {

    @Test
    void testConsumptionBasedPartitionRelease() throws IOException {
        final ResultPartitionManager manager = new ResultPartitionManager();
        final ResultPartition partition =
                new ResultPartitionBuilder()
                        .setResultPartitionType(ResultPartitionType.PIPELINED)
                        .setNumberOfSubpartitions(2)
                        .setResultPartitionManager(manager)
                        .build();

        manager.registerResultPartition(partition);

        partition.onConsumedSubpartition(0);
        assertThat(partition.isReleased()).isFalse();

        partition.onConsumedSubpartition(1);
        partition.close();
        assertThat(partition.isReleased()).isTrue();
    }

    @Test
    void testConsumptionBeforePartitionClose() throws IOException {
        final ResultPartition partition =
                new ResultPartitionBuilder()
                        .setResultPartitionType(ResultPartitionType.PIPELINED)
                        .setNumberOfSubpartitions(1)
                        .setBufferPoolFactory(UnpooledBufferPool::new)
                        .build();

        partition.setup();
        partition.emitRecord(ByteBuffer.allocate(16), 0);
        partition.onConsumedSubpartition(0);
        assertThat(partition.isReleased()).isFalse();
        partition.emitRecord(ByteBuffer.allocate(16), 0);
        partition.close();
        assertThat(partition.isReleased()).isTrue();
    }

    @Test
    void testMultipleReleaseCallsAreIdempotent() throws IOException {
        final ResultPartitionManager manager = new ResultPartitionManager();
        final ResultPartition partition =
                new ResultPartitionBuilder()
                        .setResultPartitionType(ResultPartitionType.PIPELINED)
                        .setNumberOfSubpartitions(2)
                        .setResultPartitionManager(manager)
                        .build();
        manager.registerResultPartition(partition);

        partition.onConsumedSubpartition(0);
        partition.onConsumedSubpartition(0);

        assertThat(partition.isReleased()).isFalse();
    }

    @Test
    void testReleaseAfterIdempotentCalls() throws IOException {
        final ResultPartitionManager manager = new ResultPartitionManager();
        final ResultPartition partition =
                new ResultPartitionBuilder()
                        .setResultPartitionType(ResultPartitionType.PIPELINED)
                        .setNumberOfSubpartitions(2)
                        .setResultPartitionManager(manager)
                        .build();
        manager.registerResultPartition(partition);

        partition.onConsumedSubpartition(0);
        partition.onConsumedSubpartition(0);
        partition.onConsumedSubpartition(1);
        partition.close();

        assertThat(partition.isReleased()).isTrue();
    }
}
