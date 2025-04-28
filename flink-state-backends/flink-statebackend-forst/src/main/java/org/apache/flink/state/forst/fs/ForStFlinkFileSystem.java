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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalBlockLocation;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.state.forst.fs.cache.BundledCacheLimitPolicy;
import org.apache.flink.state.forst.fs.cache.CacheLimitPolicy;
import org.apache.flink.state.forst.fs.cache.CachedDataInputStream;
import org.apache.flink.state.forst.fs.cache.CachedDataOutputStream;
import org.apache.flink.state.forst.fs.cache.FileBasedCache;
import org.apache.flink.state.forst.fs.cache.SizeBasedCacheLimitPolicy;
import org.apache.flink.state.forst.fs.cache.SpaceBasedCacheLimitPolicy;
import org.apache.flink.state.forst.fs.filemapping.FSDataOutputStreamWithEntry;
import org.apache.flink.state.forst.fs.filemapping.FileBackedMappingEntrySource;
import org.apache.flink.state.forst.fs.filemapping.FileMappingManager;
import org.apache.flink.state.forst.fs.filemapping.FileOwnership;
import org.apache.flink.state.forst.fs.filemapping.FileOwnershipDecider;
import org.apache.flink.state.forst.fs.filemapping.MappingEntry;
import org.apache.flink.state.forst.fs.filemapping.MappingEntrySource;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.state.forst.ForStConfigurableOptions.TARGET_FILE_SIZE_BASE;

/**
 * A {@link FileSystem} delegates some requests to file system loaded by Flink FileSystem mechanism.
 *
 * <p>All methods in this class maybe used by ForSt, please start a discussion firstly if it has to
 * be modified.
 */
public class ForStFlinkFileSystem extends FileSystem implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ForStFlinkFileSystem.class);

    // TODO: make it configurable
    private static final int DEFAULT_INPUT_STREAM_CAPACITY = 32;

    private final FileSystem localFS;
    private final FileSystem delegateFS;
    private final String remoteBase;
    @Nullable private final FileBasedCache fileBasedCache;
    @Nonnull private final FileMappingManager fileMappingManager;

    public ForStFlinkFileSystem(
            FileSystem delegateFS,
            String remoteBase,
            String localBase,
            @Nullable FileBasedCache fileBasedCache) {
        this.localFS = FileSystem.getLocalFileSystem();
        this.delegateFS = delegateFS;
        this.remoteBase = remoteBase;
        this.fileBasedCache = fileBasedCache;
        this.fileMappingManager = new FileMappingManager(delegateFS, remoteBase, localBase);
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
            ReadableConfig config,
            Path cacheBase,
            Path remoteForStPath,
            long cacheCapacity,
            long cacheReservedSize,
            MetricGroup metricGroup)
            throws IOException {
        if (cacheBase == null || cacheCapacity <= 0 && cacheReservedSize <= 0) {
            return null;
        }
        if (cacheBase.getFileSystem().equals(remoteForStPath.getFileSystem())) {
            LOG.info(
                    "Skip creating ForSt cache "
                            + "since the cache and primary path are on the same file system.");
            return null;
        }
        // Create cache directory to enforce SpaceBasedCacheLimitPolicy.
        if (!cacheBase.getFileSystem().mkdirs(cacheBase)) {
            throw new IOException(
                    String.format("Could not create ForSt cache directory at %s.", cacheBase));
        }
        CacheLimitPolicy cacheLimitPolicy = null;
        long targetSstFileSize = config.get(TARGET_FILE_SIZE_BASE).getBytes();
        boolean useSizeBasedCache = cacheCapacity > 0;
        // We may encounter the case that the SpaceBasedCacheLimitPolicy cannot work properly on
        // the file system, so we need to check if it works.
        boolean useSpaceBasedCache =
                cacheReservedSize > 0
                        && SpaceBasedCacheLimitPolicy.worksOn(new File(cacheBase.toString()));
        if (useSizeBasedCache && useSpaceBasedCache) {
            cacheLimitPolicy =
                    new BundledCacheLimitPolicy(
                            new SizeBasedCacheLimitPolicy(cacheCapacity, targetSstFileSize),
                            new SpaceBasedCacheLimitPolicy(
                                    new File(cacheBase.toString()),
                                    cacheReservedSize,
                                    targetSstFileSize));
        } else if (useSizeBasedCache) {
            cacheLimitPolicy = new SizeBasedCacheLimitPolicy(cacheCapacity, targetSstFileSize);
        } else if (useSpaceBasedCache) {
            cacheLimitPolicy =
                    new SpaceBasedCacheLimitPolicy(
                            new File(cacheBase.toString()), cacheReservedSize, targetSstFileSize);
        } else {
            return null;
        }
        return new FileBasedCache(
                config,
                cacheLimitPolicy,
                getUnguardedFileSystem(cacheBase),
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
        MappingEntry createdMappingEntry =
                fileMappingManager.createNewFile(
                        dbFilePath, overwriteMode == WriteMode.OVERWRITE, fileBasedCache);

        // The source must be backed by a file
        FileBackedMappingEntrySource source =
                (FileBackedMappingEntrySource) createdMappingEntry.getSource();
        Path sourceRealPath = source.getFilePath();

        // Create the actual file output stream
        // Should use the one WITHOUT safety net protection. The reason is that the ForSt LOG file
        // might be created by any thread but share among all the threads, so we cannot let the LOG
        // file auto-closed by one thread's quit.
        FileSystem fileSystem = getUnguardedFileSystem(sourceRealPath);
        FSDataOutputStream outputStream = fileSystem.create(sourceRealPath, overwriteMode);
        // Bundle the output stream with the mapping entry, to close the entry when the stream is
        // closed.
        outputStream = new FSDataOutputStreamWithEntry(outputStream, createdMappingEntry);

        // Try to create file cache for SST files
        CachedDataOutputStream cachedDataOutputStream =
                createCachedDataOutputStream(
                        dbFilePath,
                        sourceRealPath,
                        outputStream,
                        createdMappingEntry.getFileOwnership());

        LOG.trace(
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
                            createCachedDataInputStream(
                                    dbFilePath,
                                    source,
                                    inputStream,
                                    mappingEntry.getFileOwnership());
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
                            createCachedDataInputStream(
                                    dbFilePath,
                                    source,
                                    inputStream,
                                    mappingEntry.getFileOwnership());
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

        if (FileOwnershipDecider.shouldAlwaysBeLocal(f, mappingEntry.getFileOwnership())) {
            return localFS.exists(mappingEntry.getSourcePath());
        } else {
            // Should be protected with synchronized, since the file closing is not an atomic
            // operation, see FSDataOutputStreamWithEntry.close()
            synchronized (mappingEntry) {
                if (mappingEntry.isWriting()) {
                    return true;
                }
            }
            return delegateFS.exists(mappingEntry.getSourcePath());
        }
    }

    @Override
    public synchronized FileStatus getFileStatus(Path path) throws IOException {
        MappingEntry mappingEntry = fileMappingManager.mappingEntry(path.toString());
        if (mappingEntry == null) {
            return new FileStatusWrapper(delegateFS.getFileStatus(path), path);
        }
        if (FileOwnershipDecider.shouldAlwaysBeLocal(path, mappingEntry.getFileOwnership())) {
            return new FileStatusWrapper(localFS.getFileStatus(mappingEntry.getSourcePath()), path);
        } else {
            // Should be protected with synchronized, since the file closing is not an atomic
            // operation, see FSDataOutputStreamWithEntry.close()
            synchronized (mappingEntry) {
                if (mappingEntry.isWriting()) {
                    return new DummyFSFileStatus(path);
                }
            }
            return new FileStatusWrapper(
                    delegateFS.getFileStatus(mappingEntry.getSourcePath()), path);
        }
    }

    @Override
    public synchronized BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
            throws IOException {
        Path path = file.getPath();
        if (file instanceof FileStatusWrapper) {
            if (FileOwnershipDecider.shouldAlwaysBeLocal(path)) {
                return localFS.getFileBlockLocations(
                        ((FileStatusWrapper) file).delegate, start, len);
            } else {
                return delegateFS.getFileBlockLocations(
                        ((FileStatusWrapper) file).delegate, start, len);
            }
        } else if (file instanceof DummyFSFileStatus) {
            return new BlockLocation[] {new LocalBlockLocation(0L)};
        } else {
            throw new IOException("file is not an instance from ForStFlinkFileSystem.");
        }
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
        return fileMappingManager.deleteFileOrDirectory(path, recursive);
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
        MappingEntry mappingEntry =
                fileMappingManager.registerReusedRestoredFile(
                        key, stateHandle, dbFilePath, fileBasedCache);
    }

    public synchronized @Nullable MappingEntry getMappingEntry(Path path) {
        return fileMappingManager.mappingEntry(path.toString());
    }

    public synchronized void giveUpOwnership(Path path, StreamStateHandle stateHandle) {
        fileMappingManager.giveUpOwnership(path, stateHandle);
    }

    private static FileSystem getUnguardedFileSystem(Path path) throws IOException {
        return FileSystem.getUnguardedFileSystem(path.toUri());
    }

    private @Nullable CachedDataOutputStream createCachedDataOutputStream(
            Path dbFilePath,
            Path srcRealPath,
            FSDataOutputStream outputStream,
            FileOwnership fileOwnership)
            throws IOException {
        // do not create cache for local files
        if (FileOwnershipDecider.shouldAlwaysBeLocal(dbFilePath, fileOwnership)) {
            return null;
        }

        return fileBasedCache == null ? null : fileBasedCache.create(outputStream, srcRealPath);
    }

    private @Nullable CachedDataInputStream createCachedDataInputStream(
            Path dbFilePath,
            MappingEntrySource source,
            FSDataInputStream inputStream,
            FileOwnership fileOwnership)
            throws IOException {
        if (FileOwnershipDecider.shouldAlwaysBeLocal(dbFilePath, fileOwnership)
                || !source.cacheable()) {
            return null;
        }

        return fileBasedCache == null
                ? null
                : fileBasedCache.open(source.getFilePath(), inputStream);
    }

    @Override
    public void close() throws IOException {
        if (fileBasedCache != null) {
            fileBasedCache.close();
        }
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

    /** A dummy file status that only confirms the existence. */
    static class DummyFSFileStatus implements FileStatus {
        private final Path path;

        DummyFSFileStatus(Path path) {
            this.path = path;
        }

        @Override
        public long getLen() {
            return 0L;
        }

        @Override
        public long getBlockSize() {
            return 0L;
        }

        @Override
        public short getReplication() {
            return 0;
        }

        @Override
        public long getModificationTime() {
            return 0;
        }

        @Override
        public long getAccessTime() {
            return 0;
        }

        @Override
        public boolean isDir() {
            return false;
        }

        @Override
        public Path getPath() {
            return path;
        }
    }
}
