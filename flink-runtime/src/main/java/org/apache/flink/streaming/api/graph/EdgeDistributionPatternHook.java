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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.EdgeDistributionPatternSnapshot;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A {@link MasterTriggerRestoreHook} that persists the current job's edge {@link
 * org.apache.flink.runtime.jobgraph.DistributionPattern}s into checkpoint metadata, so they are
 * available when restoring after a shuffle-mode change.
 */
class EdgeDistributionPatternHook implements MasterTriggerRestoreHook<byte[]> {

    private final byte[] snapshotBytes;

    EdgeDistributionPatternHook(byte[] snapshotBytes) {
        this.snapshotBytes = snapshotBytes;
    }

    @Override
    public String getIdentifier() {
        return EdgeDistributionPatternSnapshot.HOOK_IDENTIFIER;
    }

    @Nullable
    @Override
    public CompletableFuture<byte[]> triggerCheckpoint(
            long checkpointId, long timestamp, Executor executor) {
        return CompletableFuture.completedFuture(snapshotBytes);
    }

    @Override
    public void restoreCheckpoint(long checkpointId, @Nullable byte[] checkpointData) {
        // no-op: data is extracted directly from MasterState before assignStates()
    }

    @Nullable
    @Override
    public SimpleVersionedSerializer<byte[]> createCheckpointDataSerializer() {
        return ByteArraySerializer.INSTANCE;
    }

    private static final class ByteArraySerializer implements SimpleVersionedSerializer<byte[]> {
        static final ByteArraySerializer INSTANCE = new ByteArraySerializer();

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(byte[] obj) throws IOException {
            return obj;
        }

        @Override
        public byte[] deserialize(int version, byte[] serialized) throws IOException {
            return serialized;
        }
    }

    static class Factory implements MasterTriggerRestoreHook.Factory {
        private static final long serialVersionUID = 1L;

        private final byte[] snapshotBytes;

        Factory(EdgeDistributionPatternSnapshot snapshot) throws IOException {
            this.snapshotBytes = snapshot.toBytes();
        }

        @SuppressWarnings("unchecked")
        @Override
        public <V> MasterTriggerRestoreHook<V> create() {
            return (MasterTriggerRestoreHook<V>) new EdgeDistributionPatternHook(snapshotBytes);
        }
    }
}
