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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.EntropyInjector;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A storage location for checkpoints on a file system. */
public class FsCheckpointStorageLocation extends FsCheckpointStreamFactory
        implements CheckpointStorageLocation {

    private final FileSystem fileSystem;

    private final Path checkpointDirectory;

    private final Path sharedStateDirectory;

    private final Path taskOwnedStateDirectory;

    private final Path metadataFilePath;

    private final CheckpointStorageLocationReference reference;

    private final int fileStateSizeThreshold;

    private final int writeBufferSize;

    public FsCheckpointStorageLocation(
            FileSystem fileSystem,
            Path checkpointDir,
            Path sharedStateDir,
            Path taskOwnedStateDir,
            CheckpointStorageLocationReference reference,
            int fileStateSizeThreshold,
            int writeBufferSize) {

        super(fileSystem, checkpointDir, sharedStateDir, fileStateSizeThreshold, writeBufferSize);

        checkArgument(fileStateSizeThreshold >= 0);
        checkArgument(writeBufferSize >= 0);

        this.fileSystem = checkNotNull(fileSystem);
        this.checkpointDirectory = checkNotNull(checkpointDir);
        this.sharedStateDirectory = checkNotNull(sharedStateDir);
        this.taskOwnedStateDirectory = checkNotNull(taskOwnedStateDir);
        this.reference = checkNotNull(reference);

        // the metadata file should not have entropy in its path
        Path metadataDir = EntropyInjector.removeEntropyMarkerIfPresent(fileSystem, checkpointDir);

        this.metadataFilePath =
                new Path(metadataDir, AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME);
        this.fileStateSizeThreshold = fileStateSizeThreshold;
        this.writeBufferSize = writeBufferSize;
    }

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public Path getCheckpointDirectory() {
        return checkpointDirectory;
    }

    public Path getSharedStateDirectory() {
        return sharedStateDirectory;
    }

    public Path getTaskOwnedStateDirectory() {
        return taskOwnedStateDirectory;
    }

    public Path getMetadataFilePath() {
        return metadataFilePath;
    }

    // ------------------------------------------------------------------------
    //  checkpoint metadata
    // ------------------------------------------------------------------------

    @Override
    public CheckpointMetadataOutputStream createMetadataOutputStream() throws IOException {
        return new FsCheckpointMetadataOutputStream(
                fileSystem, metadataFilePath, checkpointDirectory);
    }

    @Override
    public void disposeOnFailure() throws IOException {
        // on a failure, no chunk in the checkpoint directory needs to be saved, so
        // we can drop it as a whole
        fileSystem.delete(checkpointDirectory, true);
    }

    @Override
    public CheckpointStorageLocationReference getLocationReference() {
        return reference;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "FsCheckpointStorageLocation {"
                + "fileSystem="
                + fileSystem
                + ", checkpointDirectory="
                + checkpointDirectory
                + ", sharedStateDirectory="
                + sharedStateDirectory
                + ", taskOwnedStateDirectory="
                + taskOwnedStateDirectory
                + ", metadataFilePath="
                + metadataFilePath
                + ", reference="
                + reference
                + ", fileStateSizeThreshold="
                + fileStateSizeThreshold
                + ", writeBufferSize="
                + writeBufferSize
                + '}';
    }

    @VisibleForTesting
    FileSystem getFileSystem() {
        return fileSystem;
    }
}
