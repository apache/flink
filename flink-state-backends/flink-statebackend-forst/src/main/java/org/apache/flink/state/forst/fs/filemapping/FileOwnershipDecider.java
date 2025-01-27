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

package org.apache.flink.state.forst.fs.filemapping;

import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorageAccess;
import org.apache.flink.runtime.state.filesystem.FsMergingCheckpointStorageAccess;

import javax.annotation.Nullable;

import java.io.IOException;

public class FileOwnershipDecider {

    public static final String SST_SUFFIX = ".sst";

    private final RecoveryClaimMode recoveryClaimMode;

    public static FileOwnershipDecider getDefault() {
        return new FileOwnershipDecider(RecoveryClaimMode.DEFAULT);
    }

    private static boolean isDbPathUnderCheckpointPath(
            @Nullable CheckpointStorageAccess checkpointStorageAccess,
            @Nullable Path dbRemotePath) {
        if (dbRemotePath == null) {
            return false;
        }

        // For checkpoint other that FsCheckpointStorageAccess, we treat it as 'DB path not under
        // checkpoint path', since we cannot reuse checkpoint files in such case.
        // todo: Support enabling 'cp file reuse' with FsMergingCheckpointStorageAccess
        if (!(checkpointStorageAccess instanceof FsCheckpointStorageAccess)
                || checkpointStorageAccess instanceof FsMergingCheckpointStorageAccess) {
            return false;
        }

        FsCheckpointStorageAccess fsCheckpointStorageAccess =
                (FsCheckpointStorageAccess) checkpointStorageAccess;
        FileSystem checkpointFS = fsCheckpointStorageAccess.getFileSystem();
        FileSystem dbRemoteFS;
        try {
            dbRemoteFS = dbRemotePath.getFileSystem();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to get FileSystem from dbRemotePath: " + dbRemotePath, e);
        }

        if (!checkpointFS.equals(dbRemoteFS)) {
            // different file system
            return false;
        } else {
            // same file system
            String checkpointPathStr =
                    fsCheckpointStorageAccess.getCheckpointsDirectory().getPath();
            String dbRemotePathStr = dbRemotePath.getPath();
            return checkpointPathStr.equals(dbRemotePathStr)
                    || dbRemotePathStr.startsWith(checkpointPathStr);
        }
    }

    public FileOwnershipDecider(RecoveryClaimMode recoveryClaimMode) {
        this.recoveryClaimMode = recoveryClaimMode;
    }

    public FileOwnership decideForNewFile(Path filePath) {
        // local files are always privately owned by DB
        return shouldAlwaysBeLocal(filePath)
                ? FileOwnership.PRIVATE_OWNED_BY_DB
                : FileOwnership.SHAREABLE_OWNED_BY_DB;
    }

    public FileOwnership decideForRestoredFile(Path filePath) {
        // local files are always privately owned by DB
        return shouldAlwaysBeLocal(filePath)
                ? FileOwnership.PRIVATE_OWNED_BY_DB
                : FileOwnership.NOT_OWNED;
    }

    public static boolean isSstFile(Path filePath) {
        return filePath.getName().endsWith(SST_SUFFIX);
    }

    public static boolean shouldAlwaysBeLocal(Path filePath) {
        return !isSstFile(filePath);
    }

    @Override
    public String toString() {
        return "FileOwnershipDecider{" + "recoveryClaimMode=" + recoveryClaimMode + '}';
    }
}
