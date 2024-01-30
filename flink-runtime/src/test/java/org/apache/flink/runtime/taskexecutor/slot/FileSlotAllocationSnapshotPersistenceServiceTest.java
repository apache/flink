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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Tests for the {@link FileSlotAllocationSnapshotPersistenceService}. */
class FileSlotAllocationSnapshotPersistenceServiceTest {

    @TempDir private File tempDirectory;

    @Test
    void loadNoSlotAllocationSnapshotsIfDirectoryIsEmpty() throws IOException {
        assumeTrue(FileUtils.isEmptyDirectory(tempDirectory));

        final FileSlotAllocationSnapshotPersistenceService persistenceService =
                new FileSlotAllocationSnapshotPersistenceService(tempDirectory);

        assertThat(persistenceService.loadAllocationSnapshots()).isEmpty();
    }

    @Test
    void loadPersistedSlotAllocationSnapshots() throws IOException {
        final FileSlotAllocationSnapshotPersistenceService persistenceService =
                new FileSlotAllocationSnapshotPersistenceService(tempDirectory);

        final Collection<SlotAllocationSnapshot> slotAllocationSnapshots =
                createRandomSlotAllocationSnapshots(3);

        for (SlotAllocationSnapshot slotAllocationSnapshot : slotAllocationSnapshots) {
            persistenceService.persistAllocationSnapshot(slotAllocationSnapshot);
        }

        final Collection<SlotAllocationSnapshot> loadedSlotAllocationSnapshots =
                persistenceService.loadAllocationSnapshots();

        assertThat(loadedSlotAllocationSnapshots)
                .containsAll(slotAllocationSnapshots)
                .usingRecursiveComparison();
    }

    @Test
    void newInstanceLoadsPersistedSlotAllocationSnapshots() throws IOException {
        final FileSlotAllocationSnapshotPersistenceService persistenceService =
                new FileSlotAllocationSnapshotPersistenceService(tempDirectory);

        final Collection<SlotAllocationSnapshot> slotAllocationSnapshots =
                createRandomSlotAllocationSnapshots(3);

        for (SlotAllocationSnapshot slotAllocationSnapshot : slotAllocationSnapshots) {
            persistenceService.persistAllocationSnapshot(slotAllocationSnapshot);
        }

        final FileSlotAllocationSnapshotPersistenceService newPersistenceService =
                new FileSlotAllocationSnapshotPersistenceService(tempDirectory);

        final Collection<SlotAllocationSnapshot> loadedSlotAllocationSnapshots =
                newPersistenceService.loadAllocationSnapshots();

        assertThat(loadedSlotAllocationSnapshots)
                .containsAll(slotAllocationSnapshots)
                .usingRecursiveComparison();
    }

    @Test
    void deletePersistedSlotAllocationSnapshot() throws IOException {
        final FileSlotAllocationSnapshotPersistenceService persistenceService =
                new FileSlotAllocationSnapshotPersistenceService(tempDirectory);

        final SlotAllocationSnapshot singleSlotAllocationSnapshot =
                createSingleSlotAllocationSnapshot();

        persistenceService.persistAllocationSnapshot(singleSlotAllocationSnapshot);
        persistenceService.deleteAllocationSnapshot(singleSlotAllocationSnapshot.getSlotIndex());

        assertThat(persistenceService.loadAllocationSnapshots()).isEmpty();
    }

    @Test
    void deleteCorruptedSlotAllocationSnapshots() throws IOException {
        final FileSlotAllocationSnapshotPersistenceService persistenceService =
                new FileSlotAllocationSnapshotPersistenceService(tempDirectory);

        final SlotAllocationSnapshot singleSlotAllocationSnapshot =
                createSingleSlotAllocationSnapshot();

        persistenceService.persistAllocationSnapshot(singleSlotAllocationSnapshot);

        final File[] files = tempDirectory.listFiles();

        assertThat(files).hasSize(1);

        final File file = files[0];

        // corrupt the file
        FileUtils.writeByteArrayToFile(file, new byte[] {1, 2, 3, 4});

        assertThat(persistenceService.loadAllocationSnapshots()).isEmpty();
        assertThat(tempDirectory).isEmptyDirectory();
    }

    @Nonnull
    private Collection<SlotAllocationSnapshot> createRandomSlotAllocationSnapshots(int number) {
        final Collection<SlotAllocationSnapshot> result = new ArrayList<>();
        final ResourceID resourceId = ResourceID.generate();
        for (int slotIndex = 0; slotIndex < number; slotIndex++) {
            result.add(
                    new SlotAllocationSnapshot(
                            new SlotID(resourceId, slotIndex),
                            new JobID(),
                            "foobar",
                            new AllocationID(),
                            ResourceProfile.UNKNOWN));
        }

        return result;
    }

    private SlotAllocationSnapshot createSingleSlotAllocationSnapshot() {
        return Iterables.getOnlyElement(createRandomSlotAllocationSnapshots(1));
    }
}
