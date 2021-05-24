/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;

/**
 * A {@link SnapshotStrategy} that produces unified savepoints.
 *
 * <p>See {@link org.apache.flink.runtime.state.restore.FullSnapshotRestoreOperation} for an
 * explanation of the file layout of a savepoint.
 *
 * @see org.apache.flink.runtime.state.restore.FullSnapshotRestoreOperation
 */
public class SavepointSnapshotStrategy<K>
        implements SnapshotStrategy<KeyedStateHandle, FullSnapshotResources<K>> {

    private static final Logger LOG = LoggerFactory.getLogger(SavepointSnapshotStrategy.class);

    private final FullSnapshotResources<K> savepointResources;

    /**
     * Creates a {@link SavepointSnapshotStrategy} that writes a savepoint from the given {@link
     * FullSnapshotResources}.
     */
    public SavepointSnapshotStrategy(FullSnapshotResources<K> savepointResources) {
        this.savepointResources = savepointResources;
    }

    @Override
    public FullSnapshotResources<K> syncPrepareResources(long checkpointId) throws Exception {
        return savepointResources;
    }

    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            FullSnapshotResources<K> savepointResources,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {

        if (savepointResources.getMetaInfoSnapshots().isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Asynchronous savepoint performed on empty keyed state at {}. Returning null.",
                        timestamp);
            }
            return registry -> SnapshotResult.empty();
        }

        final SupplierWithException<CheckpointStreamWithResultProvider, Exception>
                checkpointStreamSupplier = () -> createSimpleStream(streamFactory);

        return new FullSnapshotAsyncWriter<>(
                CheckpointType.SAVEPOINT, checkpointStreamSupplier, savepointResources);
    }

    @Nonnull
    static CheckpointStreamWithResultProvider createSimpleStream(
            @Nonnull CheckpointStreamFactory primaryStreamFactory) throws IOException {

        CheckpointStreamFactory.CheckpointStateOutputStream primaryOut =
                primaryStreamFactory.createCheckpointStateOutputStream(
                        CheckpointedStateScope.EXCLUSIVE);

        return new CheckpointStreamWithResultProvider.PrimaryStreamOnly(primaryOut);
    }
}
