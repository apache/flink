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

package org.apache.flink.state.forst.datatransfer;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filemerging.SegmentFileStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.state.forst.fs.ForStFlinkFileSystem;
import org.apache.flink.state.forst.fs.filemapping.FileOwnership;
import org.apache.flink.state.forst.fs.filemapping.FileOwnershipDecider;
import org.apache.flink.state.forst.fs.filemapping.MappingEntry;
import org.apache.flink.state.forst.fs.filemapping.MappingEntrySource;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Objects;

/**
 * Data transfer strategy for ForSt DB with a remote DB path. When transferring data between
 * Checkpoint and DB, this strategy MAY reuse the file from Checkpoint storage.
 */
public class ReusableDataTransferStrategy extends CopyDataTransferStrategy {

    private final FileOwnershipDecider fileOwnershipDecider;

    ReusableDataTransferStrategy(
            @Nonnull FileOwnershipDecider fileOwnershipDecider, FileSystem dbFileSystem) {
        super(dbFileSystem);

        Preconditions.checkArgument(
                dbFileSystem instanceof ForStFlinkFileSystem,
                "Unexpected dbFileSystem type: "
                        + dbFileSystem.getClass()
                        + ", expected: "
                        + ForStFlinkFileSystem.class);

        this.fileOwnershipDecider = fileOwnershipDecider;
    }

    private ForStFlinkFileSystem getForStFlinkFileSystem() {
        // We do not check the type here, because it is checked in the constructor.
        return (ForStFlinkFileSystem) dbFileSystem;
    }

    private @Nullable HandleAndLocalPath reuseFileToCheckpoint(
            Path dbFilePath, ForStFlinkFileSystem forStFs) throws IOException {
        LOG.trace("Reuse file to checkpoint: {}", dbFilePath);

        // Find the real path of the file
        Path sourcePath = forStFs.srcPath(dbFilePath);
        if (sourcePath != null) {
            dbFilePath = sourcePath;
        }

        // Create a StateHandle with the real path or the restored handle
        MappingEntrySource source =
                Objects.requireNonNull(forStFs.getMappingEntry(dbFilePath)).getSource();
        StreamStateHandle stateHandle = source.toStateHandle();

        // Give file ownership to JM, i.e. DB will not delete it from now on
        forStFs.giveUpOwnership(dbFilePath, stateHandle);

        return HandleAndLocalPath.of(stateHandle, dbFilePath.getName());
    }

    @Override
    public HandleAndLocalPath transferToCheckpoint(
            Path dbFilePath,
            long maxTransferBytes,
            CheckpointStreamFactory checkpointStreamFactory,
            CheckpointedStateScope stateScope,
            CloseableRegistry closeableRegistry,
            CloseableRegistry tmpResourcesRegistry)
            throws IOException {
        if (maxTransferBytes < 0) {
            // Means transfer whole file to checkpoint storage.
            maxTransferBytes = Long.MAX_VALUE;
        }

        ForStFlinkFileSystem forStFs = getForStFlinkFileSystem();
        MappingEntry mappingEntry = forStFs.getMappingEntry(dbFilePath);
        Preconditions.checkNotNull(mappingEntry);
        FileOwnership fileOwnership = mappingEntry.getFileOwnership();
        if (fileOwnership == FileOwnership.PRIVATE_OWNED_BY_DB) {
            LOG.trace(
                    "Do not reuse file to checkpoint because the file is privately owned by DB: {}",
                    dbFilePath);
            return super.transferToCheckpoint(
                    dbFilePath,
                    maxTransferBytes,
                    checkpointStreamFactory,
                    stateScope,
                    closeableRegistry,
                    tmpResourcesRegistry);
        }
        return reuseFileToCheckpoint(dbFilePath, forStFs);
    }

    @Override
    public void transferFromCheckpoint(
            StreamStateHandle sourceHandle, Path targetPath, CloseableRegistry closeableRegistry)
            throws IOException {

        if (sourceHandle instanceof ByteStreamStateHandle) {
            LOG.trace(
                    "Not reusing file from checkpoint because it is stored in Memory: {}",
                    targetPath);
            super.transferFromCheckpoint(sourceHandle, targetPath, closeableRegistry);
            return;
        }

        FileOwnership fileOwnership = fileOwnershipDecider.decideForRestoredFile(targetPath);
        if (fileOwnership == FileOwnership.PRIVATE_OWNED_BY_DB) {
            LOG.trace(
                    "Not reusing file from checkpoint because the file is privately owned by DB: {}",
                    targetPath);
            super.transferFromCheckpoint(sourceHandle, targetPath, closeableRegistry);
            return;
        }
        reuseFileFromCheckpoint(sourceHandle, targetPath);
    }

    private @Nullable Path prepareSourcePathFromStateHandle(StreamStateHandle sourceHandle) {
        Path sourcePath = null;
        if (sourceHandle instanceof FileStateHandle) {
            sourcePath = ((FileStateHandle) sourceHandle).getFilePath();
        } else if (sourceHandle instanceof SegmentFileStateHandle) {
            sourcePath = ((SegmentFileStateHandle) sourceHandle).getFilePath();
        }
        return sourcePath;
    }

    private void reuseFileFromCheckpoint(StreamStateHandle sourceHandle, Path targetPath)
            throws IOException {
        LOG.trace("Reuse file from checkpoint: {}, {}", sourceHandle, targetPath);
        // register the source file
        String key = sourceHandle.getStreamStateHandleID().toString();
        getForStFlinkFileSystem().registerReusedRestoredFile(key, sourceHandle, targetPath);

        // link target to source
        getForStFlinkFileSystem().link(key, targetPath);
    }

    @Override
    public String toString() {
        return "ReusableDataTransferStrategy{"
                + "dbFileSystem="
                + dbFileSystem
                + ", fileOwnershipDecider="
                + fileOwnershipDecider
                + '}';
    }
}
