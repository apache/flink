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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

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

    private static final Map<String, String> remoteLocalMapping = new ConcurrentHashMap<>();
    private static final Function<String, Boolean> miscFileFilter = s -> !s.endsWith(".sst");

    private final FileSystem localFS;
    private final FileSystem delegateFS;
    private final String remoteBase;
    private final Function<String, Boolean> localFileFilter;
    private final String localBase;

    public ForStFlinkFileSystem(FileSystem delegateFS, String remoteBase, String localBase) {
        this.localFS = FileSystem.getLocalFileSystem();
        this.delegateFS = delegateFS;
        this.localFileFilter = miscFileFilter;
        this.remoteBase = remoteBase;
        this.localBase = localBase;
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
        String localBase = remoteLocalMapping.get(uri.toString());
        Preconditions.checkNotNull(localBase, "localBase is null, remote uri:" + uri);
        return new ForStFlinkFileSystem(FileSystem.get(uri), uri.toString(), localBase);
    }

    /**
     * Setup local base path for corresponding remote base path.
     *
     * @param remoteBasePath the remote base path.
     * @param localBasePath the local base path.
     */
    public static void setupLocalBasePath(String remoteBasePath, String localBasePath) {
        remoteLocalMapping.put(remoteBasePath, localBasePath);
    }

    /**
     * Unregister local base path for corresponding remote base path.
     *
     * @param remoteBasePath the remote base path.
     */
    public static void unregisterLocalBasePath(String remoteBasePath) {
        remoteLocalMapping.remove(remoteBasePath);
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
        Tuple2<Boolean, Path> localPathTuple = tryBuildLocalPath(path);
        if (localPathTuple.f0) {
            return new ByteBufferWritableFSDataOutputStream(
                    localFS.create(localPathTuple.f1, overwriteMode));
        }
        return new ByteBufferWritableFSDataOutputStream(delegateFS.create(path, overwriteMode));
    }

    @Override
    public ByteBufferReadableFSDataInputStream open(Path path, int bufferSize) throws IOException {
        Tuple2<Boolean, Path> localPathTuple = tryBuildLocalPath(path);
        if (localPathTuple.f0) {
            return new ByteBufferReadableFSDataInputStream(
                    () -> localFS.open(localPathTuple.f1, bufferSize),
                    DEFAULT_INPUT_STREAM_CAPACITY,
                    localFS.getFileStatus(localPathTuple.f1).getLen());
        }
        FileStatus fileStatus = checkNotNull(getFileStatus(path));
        return new ByteBufferReadableFSDataInputStream(
                () -> delegateFS.open(path, bufferSize),
                DEFAULT_INPUT_STREAM_CAPACITY,
                fileStatus.getLen());
    }

    @Override
    public ByteBufferReadableFSDataInputStream open(Path path) throws IOException {
        Tuple2<Boolean, Path> localPathTuple = tryBuildLocalPath(path);
        if (localPathTuple.f0) {
            return new ByteBufferReadableFSDataInputStream(
                    () -> localFS.open(localPathTuple.f1),
                    DEFAULT_INPUT_STREAM_CAPACITY,
                    localFS.getFileStatus(localPathTuple.f1).getLen());
        }
        FileStatus fileStatus = checkNotNull(getFileStatus(path));
        return new ByteBufferReadableFSDataInputStream(
                () -> delegateFS.open(path), DEFAULT_INPUT_STREAM_CAPACITY, fileStatus.getLen());
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        // The rename is not atomic for ForSt. Some FileSystems e.g. HDFS, OSS does not allow a
        // renaming if the target already exists. So, we delete the target before attempting the
        // rename.

        if (localFileFilter.apply(src.getName())) {
            Path localSrc = tryBuildLocalPath(src).f1;
            Path localDst = tryBuildLocalPath(dst).f1;
            FileStatus fileStatus = localFS.getFileStatus(localSrc);
            boolean success = localFS.rename(localSrc, localDst);
            if (!fileStatus.isDir()) {
                return success;
            }
        }

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
    public boolean exists(final Path f) throws IOException {
        Tuple2<Boolean, Path> localPathTuple = tryBuildLocalPath(f);
        if (localPathTuple.f0) {
            return localFS.exists(localPathTuple.f1);
        }
        return delegateFS.exists(f);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        Tuple2<Boolean, Path> localPathTuple = tryBuildLocalPath(path);
        if (localPathTuple.f0) {
            return localFS.getFileStatus(localPathTuple.f1);
        }
        return delegateFS.getFileStatus(path);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
            throws IOException {
        Path path = file.getPath();
        Tuple2<Boolean, Path> localPathTuple = tryBuildLocalPath(path);
        if (localPathTuple.f0) {
            FileStatus localFile = localFS.getFileStatus(localPathTuple.f1);
            return localFS.getFileBlockLocations(localFile, start, len);
        }
        return delegateFS.getFileBlockLocations(file, start, len);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        FileStatus[] localFiles = new FileStatus[0];
        Tuple2<Boolean, Path> localPathTuple = tryBuildLocalPath(path);
        if (localPathTuple.f0) {
            localFiles = localFS.listStatus(localPathTuple.f1);
        }
        int localFileNum = localFiles == null ? 0 : localFiles.length;
        FileStatus[] remoteFiles = delegateFS.listStatus(path);
        if (localFileNum == 0) {
            return remoteFiles;
        }
        int remoteFileNum = remoteFiles == null ? 0 : remoteFiles.length;
        FileStatus[] fileStatuses = new FileStatus[localFileNum + remoteFileNum];
        for (int index = 0; index < localFileNum; index++) {
            final FileStatus localFile = localFiles[index];
            fileStatuses[index] =
                    new FileStatus() {
                        @Override
                        public long getLen() {
                            return localFile.getLen();
                        }

                        @Override
                        public long getBlockSize() {
                            return localFile.getBlockSize();
                        }

                        @Override
                        public short getReplication() {
                            return localFile.getReplication();
                        }

                        @Override
                        public long getModificationTime() {
                            return localFile.getModificationTime();
                        }

                        @Override
                        public long getAccessTime() {
                            return localFile.getAccessTime();
                        }

                        @Override
                        public boolean isDir() {
                            return localFile.isDir();
                        }

                        @Override
                        public Path getPath() {
                            if (localFile.getPath().toString().length() == localBase.length()) {
                                return new Path(remoteBase);
                            }
                            return new Path(
                                    remoteBase,
                                    localFile.getPath().toString().substring(localBase.length()));
                        }
                    };
        }
        if (remoteFileNum != 0) {
            System.arraycopy(remoteFiles, 0, fileStatuses, localFileNum, remoteFileNum);
        }
        return fileStatuses;
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        boolean success = false;
        Tuple2<Boolean, Path> localPathTuple = tryBuildLocalPath(path);
        if (localPathTuple.f0) {
            success = localFS.delete(localPathTuple.f1, recursive); // delete from local
        }
        success |= delegateFS.delete(path, recursive); // and delete from remote
        return success;
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        boolean success = false;
        Tuple2<Boolean, Path> localPathTuple = tryBuildLocalPath(path);
        if (localPathTuple.f0) {
            success = localFS.mkdirs(localPathTuple.f1);
        }
        success &= delegateFS.mkdirs(path);
        return success;
    }

    @Override
    public boolean isDistributedFS() {
        return delegateFS.isDistributedFS();
    }

    private Tuple2<Boolean, Path> tryBuildLocalPath(Path path) {
        String remotePathStr = path.toString();
        if (localFileFilter.apply(path.getName()) && remotePathStr.startsWith(remoteBase)) {
            return Tuple2.of(
                    true,
                    remotePathStr.length() == remoteBase.length()
                            ? new Path(localBase)
                            : new Path(localBase, remotePathStr.substring(remoteBase.length())));
        }
        return Tuple2.of(false, null);
    }
}
