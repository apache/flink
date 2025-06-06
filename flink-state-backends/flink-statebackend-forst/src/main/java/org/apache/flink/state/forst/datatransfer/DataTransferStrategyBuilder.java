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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorageLocation;
import org.apache.flink.state.forst.StateHandleTransferSpec;
import org.apache.flink.state.forst.fs.ForStFlinkFileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;

/** Builder for {@link DataTransferStrategy} for snapshots. */
public class DataTransferStrategyBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(DataTransferStrategyBuilder.class);

    public static DataTransferStrategy buildForSnapshot(
            SnapshotType.SharingFilesStrategy sharingFilesStrategy,
            @Nullable ForStFlinkFileSystem forStFlinkFileSystem,
            @Nullable CheckpointStreamFactory checkpointStreamFactory,
            boolean forceCopy) {
        return buildForSnapshot(
                sharingFilesStrategy,
                forStFlinkFileSystem,
                isDbPathUnderCheckpointPathForSnapshot(
                        forStFlinkFileSystem, checkpointStreamFactory),
                forceCopy);
    }

    @VisibleForTesting
    static DataTransferStrategy buildForSnapshot(
            SnapshotType.SharingFilesStrategy sharingFilesStrategy,
            @Nullable ForStFlinkFileSystem forStFlinkFileSystem,
            boolean isDbPathUnderCheckpointPathForSnapshot,
            boolean forceCopy) {
        DataTransferStrategy strategy;
        if (forceCopy
                || sharingFilesStrategy == SnapshotType.SharingFilesStrategy.NO_SHARING
                || forStFlinkFileSystem == null
                || !isDbPathUnderCheckpointPathForSnapshot) {
            strategy =
                    forStFlinkFileSystem == null
                            ? new CopyDataTransferStrategy()
                            : new CopyDataTransferStrategy(forStFlinkFileSystem);
            LOG.info("Build DataTransferStrategy for Snapshot: {}", strategy);
            return strategy;
        }

        strategy = new ReusableDataTransferStrategy(forStFlinkFileSystem);
        LOG.info("Build DataTransferStrategy for Snapshot: {}", strategy);
        return strategy;
    }

    private static boolean isDbPathUnderCheckpointPathForSnapshot(
            @Nullable ForStFlinkFileSystem forStFlinkFileSystem,
            @Nullable CheckpointStreamFactory checkpointStreamFactory) {
        if (forStFlinkFileSystem == null) {
            return false;
        }
        // For checkpoint other than FsCheckpointStorageAccess, we treat it as 'DB path not under
        // checkpoint path', since we cannot reuse checkpoint files in such case.
        // todo: Support enabling 'cp file reuse' with FsMergingCheckpointStorageAccess
        if (checkpointStreamFactory == null
                || checkpointStreamFactory.getClass() != FsCheckpointStorageLocation.class) {
            return false;
        }

        // get checkpoint shared path
        FsCheckpointStorageLocation fsCheckpointStreamFactory =
                (FsCheckpointStorageLocation) checkpointStreamFactory;
        Path cpSharedPath = fsCheckpointStreamFactory.getTargetPath(CheckpointedStateScope.SHARED);
        FileSystem cpSharedFs;
        try {
            cpSharedFs = cpSharedPath.getFileSystem();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to get FileSystem from cpSharedPath: " + cpSharedPath, e);
        }

        // check if dbRemotePath is under cpSharedPath
        if (!cpSharedFs.getUri().equals(forStFlinkFileSystem.getDelegateFS().getUri())) {
            // different file system
            return false;
        } else {
            // same file system
            String dbRemotePathStr = forStFlinkFileSystem.getRemoteBase();
            String cpSharedPathStr = cpSharedPath.toString();
            return cpSharedPathStr.equals(dbRemotePathStr)
                    || dbRemotePathStr.startsWith(cpSharedPathStr);
        }
    }

    public static DataTransferStrategy buildForRestore(
            @Nullable ForStFlinkFileSystem forStFlinkFileSystem,
            Collection<StateHandleTransferSpec> specs,
            RecoveryClaimMode recoveryClaimMode) {
        DataTransferStrategy strategy;
        FileSystem cpSharedFs = getSharedStateFileSystem(specs);
        if (forStFlinkFileSystem == null
                || cpSharedFs == null
                || !forStFlinkFileSystem.getUri().equals(cpSharedFs.getUri())
                || recoveryClaimMode == RecoveryClaimMode.NO_CLAIM) {
            strategy =
                    forStFlinkFileSystem == null
                            ? new CopyDataTransferStrategy()
                            : new CopyDataTransferStrategy(forStFlinkFileSystem);
            LOG.info(
                    "Build DataTransferStrategy for Restore: {}, forStFlinkFileSystem: {}, cpSharedFs:{}, recoveryClaimMode:{}",
                    strategy,
                    forStFlinkFileSystem,
                    cpSharedFs,
                    recoveryClaimMode);
            return strategy;
        }

        strategy = new ReusableDataTransferStrategy(forStFlinkFileSystem);
        LOG.info("Build DataTransferStrategy for Restore: {}", strategy);
        return strategy;
    }

    private static @Nullable FileSystem getSharedStateFileSystem(
            Collection<StateHandleTransferSpec> specs) {
        for (StateHandleTransferSpec spec : specs) {
            IncrementalRemoteKeyedStateHandle stateHandle = spec.getStateHandle();
            for (IncrementalKeyedStateHandle.HandleAndLocalPath handleAndLocalPath :
                    stateHandle.getSharedState()) {
                StreamStateHandle handle = handleAndLocalPath.getHandle();
                if (handle instanceof FileStateHandle) {
                    Path dbRemotePath = ((FileStateHandle) handle).getFilePath();
                    try {
                        return dbRemotePath.getFileSystem();
                    } catch (IOException e) {
                        throw new RuntimeException(
                                "Failed to get FileSystem from handle: " + handle, e);
                    }
                }
            }
        }
        return null;
    }
}
