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

package org.apache.flink.runtime.rest.handler.legacy.checkpoints;

import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.CheckpointStatsStatus;
import org.apache.flink.runtime.rest.handler.job.checkpoints.CheckpointStatsCache;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for the CheckpointStatsCache. */
class CheckpointStatsCacheTest {

    @Test
    void testZeroSizeCache() throws Exception {
        AbstractCheckpointStats checkpoint = createCheckpoint(0, CheckpointStatsStatus.COMPLETED);

        CheckpointStatsCache cache = new CheckpointStatsCache(0);
        cache.tryAdd(checkpoint);
        assertThat(cache.tryGet(0L)).isNull();
    }

    @Test
    void testCacheAddAndGet() throws Exception {
        AbstractCheckpointStats chk0 = createCheckpoint(0, CheckpointStatsStatus.COMPLETED);
        AbstractCheckpointStats chk1 = createCheckpoint(1, CheckpointStatsStatus.COMPLETED);
        AbstractCheckpointStats chk2 = createCheckpoint(2, CheckpointStatsStatus.IN_PROGRESS);

        CheckpointStatsCache cache = new CheckpointStatsCache(1);
        cache.tryAdd(chk0);
        assertThat(cache.tryGet(0)).isEqualTo(chk0);

        cache.tryAdd(chk1);
        assertThat(cache.tryGet(0)).isNull();
        assertThat(cache.tryGet(1)).isEqualTo(chk1);

        cache.tryAdd(chk2);
        assertThat(cache.tryGet(2)).isNull();
        assertThat(cache.tryGet(0)).isNull();
        assertThat(cache.tryGet(1)).isEqualTo(chk1);
    }

    private AbstractCheckpointStats createCheckpoint(long id, CheckpointStatsStatus status) {
        AbstractCheckpointStats checkpoint = mock(AbstractCheckpointStats.class);
        when(checkpoint.getCheckpointId()).thenReturn(id);
        when(checkpoint.getStatus()).thenReturn(status);
        return checkpoint;
    }
}
