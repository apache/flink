/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * This class encapsulates the completed configuration for local recovery, i.e. the root directories
 * into which all file-based snapshots can be written and the general mode for the local recover
 * feature.
 */
public class LocalRecoveryConfig {

    public static final LocalRecoveryConfig BACKUP_AND_RECOVERY_DISABLED =
            new LocalRecoveryConfig(false, false, null);

    /** Whether to recover from the local snapshot. */
    private final boolean localRecoveryEnabled;

    /** Whether to do backup checkpoint on local disk. */
    private final boolean localBackupEnabled;

    /** Encapsulates the root directories and the subtask-specific path. */
    @Nullable private final LocalSnapshotDirectoryProvider localStateDirectories;

    public LocalRecoveryConfig(
            boolean localRecoveryEnabled,
            boolean localBackupEnabled,
            @Nullable LocalSnapshotDirectoryProvider directoryProvider) {
        this.localRecoveryEnabled = localRecoveryEnabled;
        this.localBackupEnabled = localBackupEnabled;
        this.localStateDirectories = directoryProvider;
    }

    public boolean isLocalRecoveryEnabled() {
        return localRecoveryEnabled;
    }

    public boolean isLocalBackupEnabled() {
        return localBackupEnabled;
    }

    public boolean isLocalRecoveryOrLocalBackupEnabled() {
        return localRecoveryEnabled || localBackupEnabled;
    }

    public Optional<LocalSnapshotDirectoryProvider> getLocalStateDirectoryProvider() {
        return Optional.ofNullable(localStateDirectories);
    }

    @Override
    public String toString() {
        return "LocalRecoveryConfig{" + "localStateDirectories=" + localStateDirectories + '}';
    }

    public static Supplier<IllegalStateException> localRecoveryNotEnabled() {
        return () ->
                new IllegalStateException(
                        "Getting a LocalRecoveryDirectoryProvider is only supported with the local recovery enabled. This is a bug and should be reported.");
    }

    public static LocalRecoveryConfig backupAndRecoveryEnabled(
            @Nonnull LocalSnapshotDirectoryProvider directoryProvider) {
        return new LocalRecoveryConfig(true, true, Preconditions.checkNotNull(directoryProvider));
    }
}
