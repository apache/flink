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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class ShuffleMasterSnapshotUtilTest {

    private ShuffleMaster<?> shuffleMaster;

    private Configuration configuration;

    private ShuffleMasterSnapshot restoredSnapshot;

    private boolean triggeredSnapshot;

    @TempDir java.nio.file.Path temporaryFolder;

    @BeforeEach
    public void setUp() {
        shuffleMaster =
                new NettyShuffleMaster(
                        new ShuffleMasterContextImpl(new Configuration(), throwable -> {})) {

                    @Override
                    public void snapshotState(
                            CompletableFuture<ShuffleMasterSnapshot> snapshotFuture) {
                        snapshotFuture.complete(new TestingShuffleMasterSnapshot());
                        triggeredSnapshot = true;
                    }

                    @Override
                    public void restoreState(ShuffleMasterSnapshot snapshot) {
                        restoredSnapshot = snapshot;
                    }
                };
        restoredSnapshot = null;
        triggeredSnapshot = false;
        configuration = new Configuration();
        configuration.set(BatchExecutionOptions.JOB_RECOVERY_ENABLED, true);
        configuration.set(HighAvailabilityOptions.HA_STORAGE_PATH, temporaryFolder.toString());
    }

    @Test
    void testRestoreOrSnapshotShuffleMaster() throws IOException {
        String clusterId = configuration.get(HighAvailabilityOptions.HA_CLUSTER_ID);
        Path path =
                new Path(
                        HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(
                                configuration),
                        "shuffleMaster-snapshot");
        FileSystem fileSystem = path.getFileSystem();
        assertThat(fileSystem.exists(new Path(path, clusterId))).isFalse();
        assertThat(ShuffleMasterSnapshotUtil.isShuffleMasterSnapshotExist(path, clusterId))
                .isFalse();

        // Take a snapshot
        ShuffleMasterSnapshotUtil.restoreOrSnapshotShuffleMaster(
                shuffleMaster, configuration, Executors.directExecutor());

        assertThat(fileSystem.exists(new Path(path, clusterId))).isTrue();
        assertThat(ShuffleMasterSnapshotUtil.isShuffleMasterSnapshotExist(path, clusterId))
                .isTrue();

        ShuffleMasterSnapshot snapshot = ShuffleMasterSnapshotUtil.readSnapshot(path, clusterId);
        assertThat(snapshot).isInstanceOf(TestingShuffleMasterSnapshot.class);

        assertThat(restoredSnapshot).isNull();
        assertThat(triggeredSnapshot).isTrue();

        // Clear state
        this.triggeredSnapshot = false;

        // Restore shuffle master
        ShuffleMasterSnapshotUtil.restoreOrSnapshotShuffleMaster(
                shuffleMaster, configuration, Executors.directExecutor());
        assertThat(restoredSnapshot).isInstanceOf(TestingShuffleMasterSnapshot.class);
        assertThat(triggeredSnapshot).isFalse();
    }

    private static final class TestingShuffleMasterSnapshot implements ShuffleMasterSnapshot {
        @Override
        public boolean isIncremental() {
            return false;
        }
    }
}
