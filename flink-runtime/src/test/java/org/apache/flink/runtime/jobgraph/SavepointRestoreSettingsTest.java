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
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.RecoveryClaimMode;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/** Tests for {@link SavepointRestoreSettings}. */
public class SavepointRestoreSettingsTest {

    @Test
    public void testEqualsWithDifferentRestoreMode() {
        SavepointRestoreSettings claimSettings =
                SavepointRestoreSettings.forPath("/tmp", false, RecoveryClaimMode.CLAIM);
        SavepointRestoreSettings noClaimSettings =
                SavepointRestoreSettings.forPath("/tmp", false, RecoveryClaimMode.NO_CLAIM);
        assertNotEquals(claimSettings, noClaimSettings);
    }

    @Test
    void testToConfigurationWritesExplicitlySetValues() {
        SavepointRestoreSettings settings =
                SavepointRestoreSettings.forPath("/tmp/savepoint", true, RecoveryClaimMode.CLAIM);

        Configuration configuration = new Configuration();
        SavepointRestoreSettings.toConfiguration(settings, configuration);

        assertThat(configuration.get(StateRecoveryOptions.SAVEPOINT_PATH))
                .isEqualTo("/tmp/savepoint");
        assertThat(configuration.get(StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE))
                .isTrue();
        assertThat(configuration.get(StateRecoveryOptions.RESTORE_MODE))
                .isEqualTo(RecoveryClaimMode.CLAIM);
    }

    @Test
    void testToConfigurationSkipsNonExplicitlySetValues() {
        SavepointRestoreSettings settings = SavepointRestoreSettings.forPath("/tmp/savepoint");

        Configuration configuration = new Configuration();
        SavepointRestoreSettings.toConfiguration(settings, configuration);

        // Only the savepoint path should be written
        assertThat(configuration.get(StateRecoveryOptions.SAVEPOINT_PATH))
                .isEqualTo("/tmp/savepoint");
        // These should NOT be written since they were not explicitly set (null)
        assertThat(configuration.containsKey(StateRecoveryOptions.RESTORE_MODE.key())).isFalse();
        assertThat(
                        configuration.containsKey(
                                StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE.key()))
                .isFalse();
    }

    @Test
    void testToConfigurationWithPartialExplicitSettings() {
        // Only recoveryClaimMode is explicitly set, allowNonRestoredState is NOT explicitly set
        SavepointRestoreSettings settings =
                SavepointRestoreSettings.forPath(
                        "/tmp/savepoint", (Boolean) null, RecoveryClaimMode.CLAIM);

        Configuration configuration = new Configuration();
        SavepointRestoreSettings.toConfiguration(settings, configuration);

        assertThat(configuration.get(StateRecoveryOptions.SAVEPOINT_PATH))
                .isEqualTo("/tmp/savepoint");
        assertThat(configuration.get(StateRecoveryOptions.RESTORE_MODE))
                .isEqualTo(RecoveryClaimMode.CLAIM);
        assertThat(
                        configuration.containsKey(
                                StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE.key()))
                .isFalse();
    }

    @Test
    void testNoneSettingsDoNotWriteAnything() {
        Configuration configuration = new Configuration();
        SavepointRestoreSettings.toConfiguration(SavepointRestoreSettings.none(), configuration);

        assertThat(configuration.containsKey(StateRecoveryOptions.SAVEPOINT_PATH.key())).isFalse();
        assertThat(configuration.containsKey(StateRecoveryOptions.RESTORE_MODE.key())).isFalse();
        assertThat(
                        configuration.containsKey(
                                StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE.key()))
                .isFalse();
    }

    @Test
    void testFromConfigurationWithAllValuesSet() {
        Configuration configuration = new Configuration();
        configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, "/tmp/savepoint");
        configuration.set(StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, true);
        configuration.set(StateRecoveryOptions.RESTORE_MODE, RecoveryClaimMode.CLAIM);

        SavepointRestoreSettings settings =
                SavepointRestoreSettings.fromConfiguration(configuration);

        assertThat(settings.restoreSavepoint()).isTrue();
        assertThat(settings.getRestorePath()).isEqualTo("/tmp/savepoint");
        assertThat(settings.allowNonRestoredState()).isTrue();
        assertThat(settings.getRecoveryClaimMode()).isEqualTo(RecoveryClaimMode.CLAIM);
    }

    @Test
    void testFromConfigurationWithNoPath() {
        Configuration configuration = new Configuration();
        configuration.set(StateRecoveryOptions.RESTORE_MODE, RecoveryClaimMode.CLAIM);

        SavepointRestoreSettings settings =
                SavepointRestoreSettings.fromConfiguration(configuration);

        assertThat(settings.restoreSavepoint()).isFalse();
        assertThat(settings).isEqualTo(SavepointRestoreSettings.none());
    }

    @Test
    void testFromConfigurationWithOnlyPath() {
        Configuration configuration = new Configuration();
        configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, "/tmp/savepoint");

        SavepointRestoreSettings settings =
                SavepointRestoreSettings.fromConfiguration(configuration);

        assertThat(settings.restoreSavepoint()).isTrue();
        assertThat(settings.getRestorePath()).isEqualTo("/tmp/savepoint");
        // Fields use defaults since they were not explicitly configured
        assertThat(settings.allowNonRestoredState())
                .isEqualTo(StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE.defaultValue());
        assertThat(settings.getRecoveryClaimMode())
                .isEqualTo(StateRecoveryOptions.RESTORE_MODE.defaultValue());
    }

    @Test
    void testFromConfigurationRoundTripPreservesExplicitlySetFlags() {
        // When only the path is set, fromConfiguration produces settings with
        // explicitlySet=false. A subsequent toConfiguration should NOT write those fields.
        Configuration configuration = new Configuration();
        configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, "/tmp/savepoint");

        SavepointRestoreSettings settings =
                SavepointRestoreSettings.fromConfiguration(configuration);

        Configuration output = new Configuration();
        SavepointRestoreSettings.toConfiguration(settings, output);

        assertThat(output.get(StateRecoveryOptions.SAVEPOINT_PATH)).isEqualTo("/tmp/savepoint");
        assertThat(output.containsKey(StateRecoveryOptions.RESTORE_MODE.key())).isFalse();
        assertThat(output.containsKey(StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE.key()))
                .isFalse();
    }
}
