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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.RecoveryClaimMode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Savepoint restore settings. */
public class SavepointRestoreSettings implements Serializable {

    private static final long serialVersionUID = 87377506900849777L;

    /** No restore should happen. */
    private static final SavepointRestoreSettings NONE =
            new SavepointRestoreSettings(null, null, null);

    /** Savepoint restore path. */
    private final @Nullable String restorePath;

    /**
     * Flag indicating whether non restored state is allowed if the savepoint contains state for an
     * operator that is not part of the job.
     */
    private final boolean allowNonRestoredState;

    /** Whether {@link #allowNonRestoredState} was explicitly set by the user. */
    private final boolean allowNonRestoredStateExplicitlySet;

    private final @Nullable RecoveryClaimMode recoveryClaimMode;

    /**
     * Creates the restore settings.
     *
     * @param restorePath Savepoint restore path.
     * @param allowNonRestoredState Ignore unmapped state, or {@code null} if not explicitly set.
     * @param recoveryClaimMode how to restore from the savepoint, or {@code null} if not explicitly
     *     set.
     */
    private SavepointRestoreSettings(
            @Nullable String restorePath,
            @Nullable Boolean allowNonRestoredState,
            @Nullable RecoveryClaimMode recoveryClaimMode) {
        this.restorePath = restorePath;
        this.allowNonRestoredState =
                allowNonRestoredState != null
                        ? allowNonRestoredState
                        : StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE.defaultValue();
        this.recoveryClaimMode = recoveryClaimMode;
        this.allowNonRestoredStateExplicitlySet = allowNonRestoredState != null;
    }

    /**
     * Returns whether to restore from savepoint.
     *
     * @return <code>true</code> if should restore from savepoint.
     */
    public boolean restoreSavepoint() {
        return restorePath != null;
    }

    /**
     * Returns the path to the savepoint to restore from.
     *
     * @return Path to the savepoint to restore from or <code>null</code> if should not restore.
     */
    public String getRestorePath() {
        return restorePath;
    }

    /**
     * Returns whether non restored state is allowed if the savepoint contains state that cannot be
     * mapped back to the job.
     *
     * @return <code>true</code> if non restored state is allowed if the savepoint contains state
     *     that cannot be mapped back to the job.
     */
    public boolean allowNonRestoredState() {
        return allowNonRestoredState;
    }

    /** Tells how to restore from the given savepoint. */
    public RecoveryClaimMode getRecoveryClaimMode() {
        return recoveryClaimMode != null
                ? recoveryClaimMode
                : StateRecoveryOptions.RESTORE_MODE.defaultValue();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SavepointRestoreSettings that = (SavepointRestoreSettings) o;
        return allowNonRestoredState == that.allowNonRestoredState
                && allowNonRestoredStateExplicitlySet == that.allowNonRestoredStateExplicitlySet
                && Objects.equals(restorePath, that.restorePath)
                && Objects.equals(recoveryClaimMode, that.recoveryClaimMode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                restorePath,
                allowNonRestoredState,
                allowNonRestoredStateExplicitlySet,
                recoveryClaimMode);
    }

    @Override
    public String toString() {
        if (restoreSavepoint()) {
            return "SavepointRestoreSettings.forPath("
                    + "restorePath='"
                    + restorePath
                    + '\''
                    + ", allowNonRestoredState="
                    + allowNonRestoredState()
                    + ", recoveryClaimMode="
                    + getRecoveryClaimMode()
                    + ')';
        } else {
            return "SavepointRestoreSettings.none()";
        }
    }

    // ------------------------------------------------------------------------

    public static SavepointRestoreSettings none() {
        return NONE;
    }

    public static SavepointRestoreSettings forPath(@Nonnull String savepointPath) {
        return forPath(savepointPath, null, null);
    }

    public static SavepointRestoreSettings forPath(
            @Nonnull String savepointPath, boolean allowNonRestoredState) {
        return forPath(savepointPath, allowNonRestoredState, null);
    }

    /**
     * Creates restore settings. Parameters that are {@code null} indicate the user did not
     * explicitly set them — their defaults will be used at runtime and they will not be written to
     * configuration by {@link #toConfiguration}, allowing downstream configuration (e.g., SQL SET
     * statements or flink-conf.yaml) to take effect.
     *
     * @param savepointPath the savepoint path to restore from.
     * @param allowNonRestoredState whether to allow non-restored state, or {@code null} if not
     *     explicitly set by the user.
     * @param recoveryClaimMode how to restore from the savepoint, or {@code null} if not explicitly
     *     set by the user.
     */
    public static SavepointRestoreSettings forPath(
            @Nonnull String savepointPath,
            @Nullable Boolean allowNonRestoredState,
            @Nullable RecoveryClaimMode recoveryClaimMode) {
        checkNotNull(savepointPath, "Savepoint restore path.");
        return new SavepointRestoreSettings(
                savepointPath, allowNonRestoredState, recoveryClaimMode);
    }

    // -------------------------- Parsing to and from a configuration object
    // ------------------------------------

    public static void toConfiguration(
            final SavepointRestoreSettings savepointRestoreSettings,
            final Configuration configuration) {
        if (savepointRestoreSettings.allowNonRestoredStateExplicitlySet) {
            configuration.set(
                    StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE,
                    savepointRestoreSettings.allowNonRestoredState);
        }
        if (savepointRestoreSettings.recoveryClaimMode != null) {
            configuration.set(
                    StateRecoveryOptions.RESTORE_MODE, savepointRestoreSettings.recoveryClaimMode);
        }
        final String savepointPath = savepointRestoreSettings.getRestorePath();
        if (savepointPath != null) {
            configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPath);
        }
    }

    public static SavepointRestoreSettings fromConfiguration(final ReadableConfig configuration) {
        final String savepointPath = configuration.get(StateRecoveryOptions.SAVEPOINT_PATH);
        if (savepointPath == null) {
            return SavepointRestoreSettings.none();
        }
        final Boolean allowNonRestored =
                configuration
                        .getOptional(StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE)
                        .orElse(null);
        final RecoveryClaimMode recoveryClaimMode =
                configuration.getOptional(StateRecoveryOptions.RESTORE_MODE).orElse(null);
        return SavepointRestoreSettings.forPath(savepointPath, allowNonRestored, recoveryClaimMode);
    }
}
