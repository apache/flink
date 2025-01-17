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
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.state.forst.fs.cache.BundledCacheLimitPolicy;
import org.apache.flink.state.forst.fs.cache.CacheLimitPolicy;
import org.apache.flink.state.forst.fs.cache.CachedDataInputStream;
import org.apache.flink.state.forst.fs.cache.CachedDataOutputStream;
import org.apache.flink.state.forst.fs.cache.FileBasedCache;
import org.apache.flink.state.forst.fs.cache.SizeBasedCacheLimitPolicy;
import org.apache.flink.state.forst.fs.cache.SpaceBasedCacheLimitPolicy;
import org.apache.flink.state.forst.fs.filemapping.FileMappingManager;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

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

    private static final long SST_FILE_SIZE = 1024 * 1024 * 64;

    private final FileSystem localFS;
    private final FileSystem delegateFS;
    @Nullable private final FileBasedCache fileBasedCache;
    private final FileMappingManager fileMappingManager;

    public ForStFlinkFileSystem(
            FileSystem delegateFS,
            String remoteBase,
            String localBase,
            @Nullable FileBasedCache fileBasedCache) {
        this.localFS = FileSystem.getLocalFileSystem();
        this.delegateFS = delegateFS;
        this.fileBasedCache = fileBasedCache;
        this.fileMappingManager =
                new FileMappingManager(delegateFS, localFS, remoteBase, localBase);
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
    public static ForStFlinkFileSystem get(URI uri) throws IOException {
        return new ForStFlinkFileSystem(
                FileSystem.get(uri), uri.toString(), System.getProperty("java.io.tmpdir"), null);
    }

    public static ForStFlinkFileSystem get(URI uri, Path localBase, FileBasedCache fileBasedCache)
            throws IOException {
        Preconditions.checkNotNull(localBase, "localBase is null, remote uri: %s.", uri);
        return new ForStFlinkFileSystem(
                FileSystem.get(uri), uri.toString(), localBase.toString(), fileBasedCache);
    }

    public static FileBasedCache getFileBasedCache(
            Path cacheBase, long cacheCapacity, long cacheReservedSize, MetricGroup metricGroup)
            throws IOException {
        if (cacheBase == null || cacheCapacity <= 0 && cacheReservedSize <= 0) {
            return null;
        }
        CacheLimitPolicy cacheLimitPolicy = null;
        if (cacheCapacity > 0 && cacheReservedSize > 0) {
            cacheLimitPolicy =
                    new BundledCacheLimitPolicy(
                            new SizeBasedCacheLimitPolicy(cacheCapacity),
                            new SpaceBasedCacheLimitPolicy(
                                    new File(cacheBase.toString()),
                                    cacheReservedSize,
                                    SST_FILE_SIZE));
        } else if (cacheCapacity > 0) {
            cacheLimitPolicy = new SizeBasedCacheLimitPolicy(cacheCapacity);
        } else if (cacheReservedSize > 0) {
            cacheLimitPolicy =
                    new SpaceBasedCacheLimitPolicy(
                            new File(cacheBase.toString()), cacheReservedSize, SST_FILE_SIZE);
        }
        return new FileBasedCache(
                Integer.MAX_VALUE,
                cacheLimitPolicy,
                cacheBase.getFileSystem(),
                cacheBase,
                metricGroup);
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
    public synchronized ByteBufferWritableFSDataOutputStream create(
            Path path, WriteMode overwriteMode) throws IOException {
        FileMappingManager.RealPath realPath = fileMappingManager.createFile(path);
        if (realPath.isLocal) {
            return new ByteBufferWritableFSDataOutputStream(
                    localFS.create(realPath.path, overwriteMode));
        }

        FSDataOutputStream originalOutputStream = delegateFS.create(path, overwriteMode);
        CachedDataOutputStream cachedDataOutputStream =
                fileBasedCache == null ? null : fileBasedCache.create(originalOutputStream, path);
        return new ByteBufferWritableFSDataOutputStream(
                cachedDataOutputStream == null ? originalOutputStream : cachedDataOutputStream);
    }

    @Override
    public synchronized ByteBufferReadableFSDataInputStream open(Path path, int bufferSize)
            throws IOException {
        FileMappingManager.RealPath realPath = fileMappingManager.realPath(path);
        Preconditions.checkNotNull(realPath);
        if (realPath.isLocal) {
            return new ByteBufferReadableFSDataInputStream(
                    () -> localFS.open(realPath.path, bufferSize),
                    DEFAULT_INPUT_STREAM_CAPACITY,
                    localFS.getFileStatus(realPath.path).getLen());
        }
        FileStatus fileStatus = checkNotNull(getFileStatus(realPath.path));
        return new ByteBufferReadableFSDataInputStream(
                () -> {
                    FSDataInputStream inputStream = delegateFS.open(realPath.path, bufferSize);
                    CachedDataInputStream cachedDataInputStream =
                            fileBasedCache == null
                                    ? null
                                    : fileBasedCache.open(realPath.path, inputStream);
                    return cachedDataInputStream == null ? inputStream : cachedDataInputStream;
                },
                DEFAULT_INPUT_STREAM_CAPACITY,
                fileStatus.getLen());
    }

    @Override
    public synchronized ByteBufferReadableFSDataInputStream open(Path path) throws IOException {
        FileMappingManager.RealPath realPath = fileMappingManager.realPath(path);
        Preconditions.checkNotNull(realPath);
        if (realPath.isLocal) {
            return new ByteBufferReadableFSDataInputStream(
                    () -> localFS.open(realPath.path),
                    DEFAULT_INPUT_STREAM_CAPACITY,
                    localFS.getFileStatus(realPath.path).getLen());
        }
        FileStatus fileStatus = checkNotNull(getFileStatus(realPath.path));
        return new ByteBufferReadableFSDataInputStream(
                () -> {
                    FSDataInputStream inputStream = delegateFS.open(realPath.path);
                    CachedDataInputStream cachedDataInputStream =
                            fileBasedCache == null
                                    ? null
                                    : fileBasedCache.open(realPath.path, inputStream);
                    return cachedDataInputStream == null ? inputStream : cachedDataInputStream;
                },
                DEFAULT_INPUT_STREAM_CAPACITY,
                fileStatus.getLen());
    }

    @Override
    public synchronized boolean rename(Path src, Path dst) throws IOException {
        return fileMappingManager.renameFile(src.toString(), dst.toString());
    }

    @Override
    public synchronized Path getWorkingDirectory() {
        return delegateFS.getWorkingDirectory();
    }

    @Override
    public synchronized Path getHomeDirectory() {
        return delegateFS.getHomeDirectory();
    }

    @Override
    public synchronized URI getUri() {
        return delegateFS.getUri();
    }

    @Override
    public synchronized boolean exists(final Path f) throws IOException {
        FileMappingManager.RealPath realPath = fileMappingManager.realPath(f);
        if (realPath == null) {
            return delegateFS.exists(f) && delegateFS.getFileStatus(f).isDir();
        }

        boolean status = false;
        if (realPath.isLocal) {
            status |= localFS.exists(realPath.path);
            if (!status) {
                status = delegateFS.exists(f);
            }
        } else {
            status = delegateFS.exists(realPath.path);
        }
        return status;
    }

    @Override
    public synchronized FileStatus getFileStatus(Path path) throws IOException {
        FileMappingManager.RealPath realPath = fileMappingManager.realPath(path);
        Preconditions.checkNotNull(realPath);
        if (realPath.isLocal) {
            return localFS.getFileStatus(realPath.path);
        }
        return delegateFS.getFileStatus(realPath.path);
    }

    @Override
    public synchronized BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
            throws IOException {
        Path path = file.getPath();
        FileMappingManager.RealPath realPath = fileMappingManager.realPath(path);
        Preconditions.checkNotNull(realPath);
        if (realPath.isLocal) {
            FileStatus localFile = localFS.getFileStatus(realPath.path);
            return localFS.getFileBlockLocations(localFile, start, len);
        }
        return delegateFS.getFileBlockLocations(file, start, len);
    }

    @Override
    public synchronized FileStatus[] listStatus(Path path) throws IOException {
        // mapping files
        List<FileStatus> fileStatuses = new ArrayList<>();
        String pathStr = path.toString();
        if (!pathStr.endsWith("/")) {
            pathStr += "/";
        }
        List<String> mappingFiles = fileMappingManager.listByPrefix(pathStr);
        for (String mappingFile : mappingFiles) {
            String relativePath = mappingFile.substring(pathStr.length());
            int slashIndex = relativePath.indexOf('/');
            if (slashIndex == -1) { // direct child
                fileStatuses.add(getFileStatus(new Path(mappingFile)));
            }
        }
        return fileStatuses.toArray(new FileStatus[0]);
    }

    @Override
    public synchronized boolean delete(Path path, boolean recursive) throws IOException {
        boolean success = fileMappingManager.deleteFile(path, recursive);
        if (fileBasedCache != null) {
            // only new generated file will put into cache, no need to consider file mapping
            fileBasedCache.delete(path);
        }
        return success;
    }

    @Override
    public synchronized boolean mkdirs(Path path) throws IOException {
        return delegateFS.mkdirs(path);
    }

    @Override
    public synchronized boolean isDistributedFS() {
        return delegateFS.isDistributedFS();
    }

    public synchronized int link(Path src, Path dst) throws IOException {
        return fileMappingManager.link(src.toString(), dst.toString());
    }
}
