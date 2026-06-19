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

import org.apache.flink.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * File based {@link SlotAllocationSnapshotPersistenceService} that persists the {@link
 * SlotAllocationSnapshot} as local files.
 */
public class FileSlotAllocationSnapshotPersistenceService
        implements SlotAllocationSnapshotPersistenceService {
    private static final Logger LOG =
            LoggerFactory.getLogger(FileSlotAllocationSnapshotPersistenceService.class);

    private static final String SUFFIX = ".bin";
    private final File slotAllocationSnapshotDirectory;

    public FileSlotAllocationSnapshotPersistenceService(File slotAllocationSnapshotDirectory) {
        this.slotAllocationSnapshotDirectory = slotAllocationSnapshotDirectory;

        if (!slotAllocationSnapshotDirectory.exists()
                && !slotAllocationSnapshotDirectory.mkdirs()) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot create the slot allocation snapshot directory %s.",
                            slotAllocationSnapshotDirectory));
        }
    }

    @Override
    public void persistAllocationSnapshot(SlotAllocationSnapshot slotAllocationSnapshot)
            throws IOException {
        // Let's try to write the slot allocations on file
        final File slotAllocationSnapshotFile =
                slotAllocationFile(slotAllocationSnapshot.getSlotID().getSlotNumber());

        try (ObjectOutputStream oos =
                new ObjectOutputStream(new FileOutputStream(slotAllocationSnapshotFile))) {
            oos.writeObject(slotAllocationSnapshot);

            LOG.debug(
                    "Successfully written allocation state metadata file {} for job {} and allocation {}.",
                    slotAllocationSnapshotFile.toPath(),
                    slotAllocationSnapshot.getJobId(),
                    slotAllocationSnapshot.getAllocationId());
        }
    }

    private File slotAllocationFile(int slotIndex) {
        return new File(
                slotAllocationSnapshotDirectory.getAbsolutePath(), slotIndexToFilename(slotIndex));
    }

    private static String slotIndexToFilename(int slotIndex) {
        return slotIndex + SUFFIX;
    }

    private static int filenameToSlotIndex(String filename) {
        return Integer.parseInt(filename.substring(0, filename.length() - SUFFIX.length()));
    }

    @Override
    public void deleteAllocationSnapshot(int slotIndex) {
        // Let's try to write the slot allocations on file
        final File slotAllocationSnapshotFile = slotAllocationFile(slotIndex);
        try {
            FileUtils.deleteFileOrDirectory(slotAllocationSnapshotFile);
            LOG.debug(
                    "Successfully deleted allocation state metadata file {}.",
                    slotAllocationSnapshotFile.toPath());
        } catch (IOException ioe) {
            LOG.warn(
                    "Cannot delete the local allocations state file {}.",
                    slotAllocationSnapshotFile.toPath(),
                    ioe);
        }
    }

    @Override
    public Collection<SlotAllocationSnapshot> loadAllocationSnapshots() {
        // Let's try to populate the slot allocation from local file
        final File[] slotAllocationFiles = slotAllocationSnapshotDirectory.listFiles();
        if (slotAllocationFiles == null) {
            LOG.debug("No allocation files to load.");
            return Collections.emptyList();
        }

        Collection<SlotAllocationSnapshot> slotAllocationSnapshots =
                new ArrayList<>(slotAllocationFiles.length);

        for (File allocationFile : slotAllocationFiles) {
            try (ObjectInputStream ois =
                    new ObjectInputStream(new FileInputStream(allocationFile))) {
                slotAllocationSnapshots.add((SlotAllocationSnapshot) ois.readObject());
            } catch (IOException | ClassNotFoundException e) {
                LOG.debug(
                        "Cannot read the local allocations state file {}. Deleting it now.",
                        allocationFile.toPath(),
                        e);
                deleteAllocationSnapshot(filenameToSlotIndex(allocationFile.getName()));
            }
        }

        return slotAllocationSnapshots;
    }
}
