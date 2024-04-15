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

package org.apache.flink.runtime.state.filemerging;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.filemerging.LogicalFile;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.PhysicalStateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.FsSegmentDataInputStream;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link FileStateHandle} for state that was written to a file segment. A {@link
 * SegmentFileStateHandle} represents a {@link LogicalFile}, which has already been written to a
 * segment in a physical file.
 */
public class SegmentFileStateHandle implements StreamStateHandle {

    private static final long serialVersionUID = 1L;

    /** The path to the file in the filesystem, fully describing the file system. */
    private final Path filePath;

    /** The size of the state in the file. */
    protected final long stateSize;

    /** The starting position of the segment in the file. */
    private final long startPos;

    /** The scope of the state. */
    private final CheckpointedStateScope scope;

    /**
     * Creates a new segment file state for the given file path.
     *
     * @param filePath The path to the file that stores the state.
     * @param startPos Start position of the segment in the physical file.
     * @param stateSize Size of the segment.
     * @param scope The state's scope, whether it is exclusive or shared.
     */
    public SegmentFileStateHandle(
            Path filePath, long startPos, long stateSize, CheckpointedStateScope scope) {
        this.filePath = filePath;
        this.stateSize = stateSize;
        this.startPos = startPos;
        this.scope = scope;
    }

    /**
     * This method should be empty, so that JM is not in charge of the lifecycle of files in a
     * file-merging checkpoint.
     */
    @Override
    public void discardState() {}

    /**
     * Gets the path where this handle's state is stored.
     *
     * @return The path where this handle's state is stored.
     */
    public Path getFilePath() {
        return filePath;
    }

    @Override
    public FSDataInputStream openInputStream() throws IOException {
        FSDataInputStream inputStream = getFileSystem().open(filePath);
        return new FsSegmentDataInputStream(inputStream, startPos, stateSize);
    }

    @Override
    public Optional<byte[]> asBytesIfInMemory() {
        return Optional.empty();
    }

    @Override
    public PhysicalStateHandleID getStreamStateHandleID() {
        return new PhysicalStateHandleID(filePath.toUri().toString());
    }

    public long getStartPos() {
        return startPos;
    }

    @Override
    public long getStateSize() {
        return stateSize;
    }

    @Override
    public void collectSizeStats(StateObjectSizeStatsCollector collector) {
        collector.add(StateObjectLocation.REMOTE, getStateSize());
    }

    public CheckpointedStateScope getScope() {
        return scope;
    }

    /**
     * Gets the file system that stores the file state.
     *
     * @return The file system that stores the file state.
     * @throws IOException Thrown if the file system cannot be accessed.
     */
    private FileSystem getFileSystem() throws IOException {
        return FileSystem.get(filePath.toUri());
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof SegmentFileStateHandle)) {
            return false;
        }

        SegmentFileStateHandle that = (SegmentFileStateHandle) o;

        return filePath.equals(that.filePath)
                && startPos == that.startPos
                && stateSize == that.stateSize
                && scope.equals(that.scope);
    }

    @Override
    public int hashCode() {
        int result = getFilePath().hashCode();
        result = 31 * result + Objects.hashCode(startPos);
        result = 31 * result + Objects.hashCode(stateSize);
        result = 31 * result + Objects.hashCode(scope);
        return result;
    }

    @Override
    public String toString() {
        return String.format(
                "Segment File State: %s [Starting Position: %d, %d bytes]",
                getFilePath(), startPos, stateSize);
    }
}
