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
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.apache.flink.state.forst.StateHandleTransferSpec;
import org.apache.flink.state.forst.fs.ForStFlinkFileSystem;
import org.apache.flink.state.forst.fs.filemapping.FileOwnershipDecider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;

/** Builder for {@link DataTransferStrategy} for snapshots. */
public class DataTransferStrategyBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(DataTransferStrategyBuilder.class);

    public static DataTransferStrategy buildForSnapshot(
            @Nullable ForStFlinkFileSystem forStFlinkFileSystem,
            @Nullable CheckpointStreamFactory checkpointStreamFactory) {
        return buildForSnapshot(
                forStFlinkFileSystem,
                isDbPathUnderCheckpointPathForSnapshot(
                        forStFlinkFileSystem, checkpointStreamFactory));
    }

    @VisibleForTesting
    static DataTransferStrategy buildForSnapshot(
            @Nullable ForStFlinkFileSystem forStFlinkFileSystem,
            boolean isDbPathUnderCheckpointPathForSnapshot) {
        DataTransferStrategy strategy;
        if (forStFlinkFileSystem == null || isDbPathUnderCheckpointPathForSnapshot) {
            strategy =
                    forStFlinkFileSystem == null
                            ? new CopyDataTransferStrategy()
                            : new CopyDataTransferStrategy(forStFlinkFileSystem);
            LOG.info("Build DataTransferStrategy for Snapshot: {}", strategy);
            return strategy;
        }

        strategy =
                new ReusableDataTransferStrategy(
                        FileOwnershipDecider.getDefault(), forStFlinkFileSystem);
        LOG.info("Build DataTransferStrategy for Snapshot: {}", strategy);
        return strategy;
    }

    private static boolean isDbPathUnderCheckpointPathForSnapshot(
            @Nullable ForStFlinkFileSystem forStFlinkFileSystem,
            @Nullable CheckpointStreamFactory checkpointStreamFactory) {
        if (forStFlinkFileSystem == null) {
            return false;
        }
        // For checkpoint other that FsCheckpointStorageAccess, we treat it as 'DB path not under
        // checkpoint path', since we cannot reuse checkpoint files in such case.
        // todo: Support enabling 'cp file reuse' with FsMergingCheckpointStorageAccess
        if (checkpointStreamFactory == null
                || checkpointStreamFactory.getClass() != FsCheckpointStreamFactory.class) {
            return false;
        }

        // get checkpoint shared path
        FsCheckpointStreamFactory fsCheckpointStreamFactory =
                (FsCheckpointStreamFactory) checkpointStreamFactory;
        Path cpSharedPath = fsCheckpointStreamFactory.getTargetPath(CheckpointedStateScope.SHARED);
        FileSystem cpSharedFs;
        try {
            cpSharedFs = cpSharedPath.getFileSystem();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to get FileSystem from cpSharedPath: " + cpSharedPath, e);
        }

        // check if dbRemotePath is under cpSharedPath
        if (!cpSharedFs.equals(forStFlinkFileSystem.getDelegateFS())) {
            // different file system
            return false;
        } else {
            // same file system
            String dbRemotePathStr = forStFlinkFileSystem.getRemoteBase();
            String cpSharedPathStr = cpSharedPath.getPath();
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
                || forStFlinkFileSystem.equals(cpSharedFs)) {
            strategy =
                    forStFlinkFileSystem == null
                            ? new CopyDataTransferStrategy()
                            : new CopyDataTransferStrategy(forStFlinkFileSystem);
            LOG.info("Build DataTransferStrategy for Restore: {}", strategy);
            return strategy;
        }

        strategy =
                new ReusableDataTransferStrategy(
                        new FileOwnershipDecider(recoveryClaimMode), forStFlinkFileSystem);
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
