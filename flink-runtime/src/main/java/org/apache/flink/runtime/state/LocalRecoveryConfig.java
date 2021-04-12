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

import javax.annotation.Nonnull;

/**
 * This class encapsulates the completed configuration for local recovery, i.e. the root directories
 * into which all file-based snapshots can be written and the general mode for the local recover
 * feature.
 */
public class LocalRecoveryConfig {

    /** The local recovery mode. */
    private final boolean localRecoveryEnabled;

    /** Encapsulates the root directories and the subtask-specific path. */
    @Nonnull private final LocalRecoveryDirectoryProvider localStateDirectories;

    public LocalRecoveryConfig(
            boolean localRecoveryEnabled,
            @Nonnull LocalRecoveryDirectoryProvider directoryProvider) {
        this.localRecoveryEnabled = localRecoveryEnabled;
        this.localStateDirectories = directoryProvider;
    }

    public boolean isLocalRecoveryEnabled() {
        return localRecoveryEnabled;
    }

    @Nonnull
    public LocalRecoveryDirectoryProvider getLocalStateDirectoryProvider() {
        return localStateDirectories;
    }

    @Override
    public String toString() {
        return "LocalRecoveryConfig{"
                + "localRecoveryMode="
                + localRecoveryEnabled
                + ", localStateDirectories="
                + localStateDirectories
                + '}';
    }
}
