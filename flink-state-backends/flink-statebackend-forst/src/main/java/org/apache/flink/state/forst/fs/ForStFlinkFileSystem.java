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
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.state.forst.fs.cache.BundledCacheLimitPolicy;
import org.apache.flink.state.forst.fs.cache.CacheLimitPolicy;
import org.apache.flink.state.forst.fs.cache.CachedDataInputStream;
import org.apache.flink.state.forst.fs.cache.CachedDataOutputStream;
import org.apache.flink.state.forst.fs.cache.FileBasedCache;
import org.apache.flink.state.forst.fs.cache.SizeBasedCacheLimitPolicy;
import org.apache.flink.state.forst.fs.cache.SpaceBasedCacheLimitPolicy;
import org.apache.flink.state.forst.fs.filemapping.FileBackedMappingEntrySource;
import org.apache.flink.state.forst.fs.filemapping.FileMappingManager;
import org.apache.flink.state.forst.fs.filemapping.FileOwnershipDecider;
import org.apache.flink.state.forst.fs.filemapping.MappingEntry;
import org.apache.flink.state.forst.fs.filemapping.MappingEntrySource;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link FileSystem} delegates some requests to file system loaded by Flink FileSystem mechanism.
 *
 * <p>All methods in this class maybe used by ForSt, please start a discussion firstly if it has to
 * be modified.
 */
@Experimental
public class ForStFlinkFileSystem extends FileSystem {

    private static final Logger LOG = LoggerFactory.getLogger(ForStFlinkFileSystem.class);

    // TODO: make it configurable
    private static final int DEFAULT_INPUT_STREAM_CAPACITY = 32;

    private static final long SST_FILE_SIZE = 1024 * 1024 * 64;

    private final FileSystem localFS;
    private final FileSystem delegateFS;
    private final String remoteBase;
    @Nullable private final FileBasedCache fileBasedCache;
    @Nonnull private final FileMappingManager fileMappingManager;

    public ForStFlinkFileSystem(
            FileSystem delegateFS,
            String remoteBase,
            String localBase,
            @Nullable FileOwnershipDecider fileOwnershipDecider,
            @Nullable FileBasedCache fileBasedCache) {
        this.localFS = FileSystem.getLocalFileSystem();
        this.delegateFS = delegateFS;
        this.remoteBase = remoteBase;
        this.fileBasedCache = fileBasedCache;
        this.fileMappingManager =
                new FileMappingManager(delegateFS, fileOwnershipDecider, remoteBase, localBase);
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
                FileSystem.get(uri),
                uri.toString(),
                System.getProperty("java.io.tmpdir"),
                null,
                null);
    }

    public static ForStFlinkFileSystem get(
            URI uri,
            Path localBase,
            FileOwnershipDecider fileOwnershipDecider,
            FileBasedCache fileBasedCache)
            throws IOException {
        Preconditions.checkNotNull(localBase, "localBase is null, remote uri: %s.", uri);
        return new ForStFlinkFileSystem(
                FileSystem.get(uri),
                uri.toString(),
                localBase.toString(),
                fileOwnershipDecider,
                fileBasedCache);
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

    public FileSystem getDelegateFS() {
        return delegateFS;
    }

    public String getRemoteBase() {
        return remoteBase;
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
            Path dbFilePath, WriteMode overwriteMode) throws IOException {
        // Create a file in the mapping table
        MappingEntry createdMappingEntry = fileMappingManager.createNewFile(dbFilePath);

        // The source must be backed by a file
        FileBackedMappingEntrySource source =
                (FileBackedMappingEntrySource) createdMappingEntry.getSource();
        Path sourceRealPath = source.getFilePath();

        // Create the actual file output stream
        FileSystem fileSystem = sourceRealPath.getFileSystem();
        FSDataOutputStream outputStream = fileSystem.create(sourceRealPath, overwriteMode);

        // Try to create file cache for SST files
        CachedDataOutputStream cachedDataOutputStream =
                createCachedDataOutputStream(dbFilePath, sourceRealPath, outputStream);

        LOG.info(
                "Create file: dbFilePath: {}, sourceRealPath: {}, cachedDataOutputStream: {}",
                dbFilePath,
                sourceRealPath,
                cachedDataOutputStream);
        return new ByteBufferWritableFSDataOutputStream(
                cachedDataOutputStream == null ? outputStream : cachedDataOutputStream);
    }

    @Override
    public synchronized ByteBufferReadableFSDataInputStream open(Path dbFilePath, int bufferSize)
            throws IOException {
        MappingEntry mappingEntry = fileMappingManager.mappingEntry(dbFilePath.toString());
        Preconditions.checkNotNull(mappingEntry);
        MappingEntrySource source = mappingEntry.getSource();

        return new ByteBufferReadableFSDataInputStream(
                () -> {
                    FSDataInputStream inputStream = source.openInputStream(bufferSize);
                    CachedDataInputStream cachedDataInputStream =
                            createCachedDataInputStream(dbFilePath, source, inputStream);
                    return cachedDataInputStream == null ? inputStream : cachedDataInputStream;
                },
                DEFAULT_INPUT_STREAM_CAPACITY,
                source.getSize());
    }

    @Override
    public synchronized ByteBufferReadableFSDataInputStream open(Path dbFilePath)
            throws IOException {
        MappingEntry mappingEntry = fileMappingManager.mappingEntry(dbFilePath.toString());
        Preconditions.checkNotNull(mappingEntry);
        MappingEntrySource source = mappingEntry.getSource();

        return new ByteBufferReadableFSDataInputStream(
                () -> {
                    FSDataInputStream inputStream = source.openInputStream();
                    CachedDataInputStream cachedDataInputStream =
                            createCachedDataInputStream(dbFilePath, source, inputStream);
                    return cachedDataInputStream == null ? inputStream : cachedDataInputStream;
                },
                DEFAULT_INPUT_STREAM_CAPACITY,
                source.getSize());
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
        MappingEntry mappingEntry = fileMappingManager.mappingEntry(f.toString());
        if (mappingEntry == null) {
            return delegateFS.exists(f) && delegateFS.getFileStatus(f).isDir();
        }

        if (FileOwnershipDecider.shouldAlwaysBeLocal(f)) {
            return localFS.exists(mappingEntry.getSourcePath())
                    || delegateFS.exists(mappingEntry.getSourcePath());
        } else {
            return delegateFS.exists(mappingEntry.getSourcePath());
        }
    }

    @Override
    public synchronized FileStatus getFileStatus(Path path) throws IOException {
        Path sourcePath = getSourcePath(path);
        FileSystem fileSystem = sourcePath.getFileSystem();
        return new FileStatusWrapper(fileSystem.getFileStatus(sourcePath), path);
    }

    @Override
    public synchronized BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
            throws IOException {
        Path sourcePath = getSourcePath(file.getPath());

        FileSystem fileSystem = sourcePath.getFileSystem();
        FileStatus fileStatus = fileSystem.getFileStatus(sourcePath);
        return fileSystem.getFileBlockLocations(fileStatus, start, len);
    }

    private @Nonnull Path getSourcePath(Path path) throws FileNotFoundException {
        MappingEntry mappingEntry = fileMappingManager.mappingEntry(path.toString());
        Preconditions.checkNotNull(mappingEntry);
        MappingEntrySource source = mappingEntry.getSource();
        Path sourcePath = source.getFilePath();
        if (sourcePath == null) {
            throw new FileNotFoundException(
                    String.format("Cannot get file path for source: %s", source));
        }
        return sourcePath;
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
        boolean success = fileMappingManager.deleteFileOrDirectory(path, recursive);
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

    public synchronized int link(String src, Path dst) throws IOException {
        return fileMappingManager.link(src, dst.toString());
    }

    public synchronized void registerReusedRestoredFile(
            String key, StreamStateHandle stateHandle, Path dbFilePath) {
        fileMappingManager.registerReusedRestoredFile(key, stateHandle, dbFilePath);
    }

    public synchronized @Nullable Path srcPath(Path path) {
        MappingEntry mappingEntry = fileMappingManager.mappingEntry(path.toString());
        return mappingEntry == null ? null : mappingEntry.getSourcePath();
    }

    public synchronized @Nullable MappingEntry getMappingEntry(Path path) {
        return fileMappingManager.mappingEntry(path.toString());
    }

    public synchronized void giveUpOwnership(Path path, StreamStateHandle stateHandle) {
        fileMappingManager.giveUpOwnership(path, stateHandle);
    }

    private @Nullable CachedDataOutputStream createCachedDataOutputStream(
            Path dbFilePath, Path srcRealPath, FSDataOutputStream outputStream) throws IOException {
        // do not create cache for local files
        if (FileOwnershipDecider.shouldAlwaysBeLocal(dbFilePath)) {
            return null;
        }

        return fileBasedCache == null ? null : fileBasedCache.create(outputStream, srcRealPath);
    }

    private @Nullable CachedDataInputStream createCachedDataInputStream(
            Path dbFilePath, MappingEntrySource source, FSDataInputStream inputStream)
            throws IOException {
        if (FileOwnershipDecider.shouldAlwaysBeLocal(dbFilePath) || !source.cacheable()) {
            return null;
        }

        return fileBasedCache == null
                ? null
                : fileBasedCache.open(source.getFilePath(), inputStream);
    }

    public static class FileStatusWrapper implements FileStatus {
        private final FileStatus delegate;
        private final Path path;

        public FileStatusWrapper(FileStatus delegate, Path path) {
            this.delegate = delegate;
            this.path = path;
        }

        @Override
        public long getLen() {
            return delegate.getLen();
        }

        @Override
        public long getBlockSize() {
            return delegate.getBlockSize();
        }

        @Override
        public short getReplication() {
            return delegate.getReplication();
        }

        @Override
        public long getModificationTime() {
            return delegate.getModificationTime();
        }

        @Override
        public long getAccessTime() {
            return delegate.getAccessTime();
        }

        @Override
        public boolean isDir() {
            return delegate.isDir();
        }

        @Override
        public Path getPath() {
            return path;
        }
    }
}
