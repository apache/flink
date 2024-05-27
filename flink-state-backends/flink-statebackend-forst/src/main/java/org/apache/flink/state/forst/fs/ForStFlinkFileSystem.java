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

package org.apache.flink.state.forst.fs;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.net.URI;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link FileSystem} delegates some requests to file system loaded by Flink FileSystem mechanism.
 *
 * <p>All methods in this class maybe used by ForSt, please start a discussion firstly if it has to
 * be modified.
 */
@Experimental
public class ForStFlinkFileSystem extends FileSystem {

    // TODO: make it configurable
    private static final int DEFAULT_INPUT_STREAM_CAPACITY = 32;

    private final FileSystem delegateFS;

    public ForStFlinkFileSystem(FileSystem delegateFS) {
        this.delegateFS = delegateFS;
    }

    /**
     * Returns a reference to the {@link FileSystem} instance for accessing the file system
     * identified by the given {@link URI}.
     *
     * @param uri the {@link URI} identifying the file system.
     * @return a reference to the {@link FileSystem} instance for accessing the file system
     *     identified by the given {@link URI}.
     * @throws IOException thrown if a reference to the file system instance could not be obtained.
     */
    public static FileSystem get(URI uri) throws IOException {
        return new ForStFlinkFileSystem(FileSystem.get(uri));
    }

    /**
     * Create ByteBufferWritableFSDataOutputStream from specific path which supports to write data
     * to ByteBuffer with {@link org.apache.flink.core.fs.FileSystem.WriteMode#OVERWRITE} mode.
     *
     * @param path The file path to write to.
     * @return The stream to the new file at the target path.
     * @throws IOException Thrown, if the stream could not be opened because of an I/O, or because a
     *     file already exists at that path and the write mode indicates to not overwrite the file.
     */
    public ByteBufferWritableFSDataOutputStream create(Path path) throws IOException {
        return create(path, WriteMode.OVERWRITE);
    }

    @Override
    public ByteBufferWritableFSDataOutputStream create(Path path, WriteMode overwriteMode)
            throws IOException {
        return new ByteBufferWritableFSDataOutputStream(delegateFS.create(path, overwriteMode));
    }

    @Override
    public ByteBufferReadableFSDataInputStream open(Path path, int bufferSize) throws IOException {
        FileStatus fileStatus = checkNotNull(getFileStatus(path));
        return new ByteBufferReadableFSDataInputStream(
                () -> delegateFS.open(path, bufferSize),
                DEFAULT_INPUT_STREAM_CAPACITY,
                fileStatus.getLen());
    }

    @Override
    public ByteBufferReadableFSDataInputStream open(Path path) throws IOException {
        FileStatus fileStatus = checkNotNull(getFileStatus(path));
        return new ByteBufferReadableFSDataInputStream(
                () -> delegateFS.open(path), DEFAULT_INPUT_STREAM_CAPACITY, fileStatus.getLen());
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        // The rename is not atomic for ForSt. Some FileSystems e.g. HDFS, OSS does not allow a
        // renaming if the target already exists. So, we delete the target before attempting the
        // rename.
        if (delegateFS.exists(dst)) {
            boolean deleted = delegateFS.delete(dst, false);
            if (!deleted) {
                throw new IOException("Fail to delete dst path: " + dst);
            }
        }
        return delegateFS.rename(src, dst);
    }

    @Override
    public Path getWorkingDirectory() {
        return delegateFS.getWorkingDirectory();
    }

    @Override
    public Path getHomeDirectory() {
        return delegateFS.getHomeDirectory();
    }

    @Override
    public URI getUri() {
        return delegateFS.getUri();
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return delegateFS.getFileStatus(path);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
            throws IOException {
        return delegateFS.getFileBlockLocations(file, start, len);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return delegateFS.listStatus(path);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return delegateFS.delete(path, recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        return delegateFS.mkdirs(path);
    }

    @Override
    public boolean isDistributedFS() {
        return delegateFS.isDistributedFS();
    }

    @Override
    public FileSystemKind getKind() {
        return delegateFS.getKind();
    }
}
