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

import org.apache.flink.util.FileUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class represents a directory that is the target for a state snapshot. This class provides
 * some method that simplify resource management when dealing with such directories, e.g. it can
 * produce a {@link DirectoryStateHandle} when the snapshot is completed and disposal considers
 * whether or not a snapshot was already completed. For a completed snapshot, the ownership for
 * cleanup is transferred to the created directory state handle. For incomplete snapshots, calling
 * {@link #cleanup()} will delete the underlying directory resource.
 */
public abstract class SnapshotDirectory {

    /** Lifecycle stages of a snapshot directory. */
    enum State {
        ONGOING,
        COMPLETED,
        DELETED
    }

    /** This path describes the underlying directory for the snapshot. */
    @Nonnull protected final Path directory;

    /** This reference tracks the lifecycle state of the snapshot directory. */
    @Nonnull protected AtomicReference<State> state;

    private SnapshotDirectory(@Nonnull Path directory) {
        this.directory = directory;
        this.state = new AtomicReference<>(State.ONGOING);
    }

    @Nonnull
    public Path getDirectory() {
        return directory;
    }

    public boolean mkdirs() throws IOException {
        Files.createDirectories(directory);
        return true;
    }

    public boolean exists() throws IOException {
        return Files.exists(directory);
    }

    /**
     * List the files in the snapshot directory.
     *
     * @return the files in the snapshot directory.
     * @throws IOException if there is a problem creating the file statuses.
     */
    public Path[] listDirectory() throws IOException {
        return FileUtils.listDirectory(directory);
    }

    /**
     * Calling this method will attempt delete the underlying snapshot directory recursively, if the
     * state is "ongoing". In this case, the state will be set to "deleted" as a result of this
     * call.
     *
     * @return <code>true</code> if delete is successful, <code>false</code> otherwise.
     * @throws IOException if an exception happens during the delete.
     */
    public boolean cleanup() throws IOException {
        if (state.compareAndSet(State.ONGOING, State.DELETED)) {
            FileUtils.deleteDirectory(directory.toFile());
        }
        return true;
    }

    /** Returns <code>true</code> if the snapshot is marked as completed. */
    public boolean isSnapshotCompleted() {
        return State.COMPLETED == state.get();
    }

    /**
     * Calling this method completes the snapshot for this snapshot directory, if possible, and
     * creates a corresponding {@link DirectoryStateHandle} that points to the snapshot directory.
     * Calling this method can change the lifecycle state from ONGOING to COMPLETED if the directory
     * should no longer deleted in {@link #cleanup()}. This method can return Can return <code>true
     * </code> if the directory is temporary and should therefore not be referenced in a handle.
     *
     * @return A directory state handle that points to the snapshot directory. Can return <code>true
     *     </code> if the directory is temporary and should therefore not be referenced in a handle.
     * @throws IOException if the state of this snapshot directory object is different from
     *     "ongoing".
     */
    @Nullable
    public abstract DirectoryStateHandle completeSnapshotAndGetHandle() throws IOException;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SnapshotDirectory that = (SnapshotDirectory) o;

        return directory.equals(that.directory);
    }

    @Override
    public int hashCode() {
        return directory.hashCode();
    }

    @Override
    public String toString() {
        return "SnapshotDirectory{" + "directory=" + directory + ", state=" + state + '}';
    }

    /**
     * Creates a local temporary snapshot directory for the given path. This will always return
     * "null" as result of {@link #completeSnapshotAndGetHandle()} and always attempt to delete the
     * underlying directory in {@link #cleanup()}.
     */
    public static SnapshotDirectory temporary(@Nonnull File directory) throws IOException {
        return new TemporarySnapshotDirectory(directory);
    }

    /**
     * Creates a permanent snapshot directory for the given path, which will not delete the
     * underlying directory in {@link #cleanup()} after {@link #completeSnapshotAndGetHandle()} was
     * called.
     */
    public static SnapshotDirectory permanent(@Nonnull Path directory) throws IOException {
        return new PermanentSnapshotDirectory(directory);
    }

    private static class TemporarySnapshotDirectory extends SnapshotDirectory {

        TemporarySnapshotDirectory(@Nonnull File directory) throws IOException {
            super(directory.toPath());
        }

        @Override
        public DirectoryStateHandle completeSnapshotAndGetHandle() {
            return null; // We return null so that directory it is not referenced by a state handle.
        }
    }

    private static class PermanentSnapshotDirectory extends SnapshotDirectory {

        PermanentSnapshotDirectory(@Nonnull Path directory) throws IOException {
            super(directory);
        }

        @Override
        public DirectoryStateHandle completeSnapshotAndGetHandle() throws IOException {
            if (State.COMPLETED == state.get()
                    || state.compareAndSet(State.ONGOING, State.COMPLETED)) {
                return new DirectoryStateHandle(directory);
            } else {
                throw new IOException(
                        "Expected state " + State.ONGOING + " but found state " + state.get());
            }
        }
    }
}
