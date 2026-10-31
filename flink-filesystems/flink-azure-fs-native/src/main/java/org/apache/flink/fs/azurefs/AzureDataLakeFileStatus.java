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

package org.apache.flink.fs.azurefs;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * File status implementation for Azure Data Lake Storage Gen2.
 *
 * <p>Use the factory methods {@link #forDirectory(Path, long)} and {@link #forFile(Path, long,
 * long, long)} to create instances.
 */
@Immutable
final class AzureDataLakeFileStatus implements FileStatus {

    /**
     * Azure Data Lake Storage does not track access time. This constant is used as the default
     * until access-time support is added.
     */
    private static final long DEFAULT_ACCESS_TIME = 0L;

    private final long length;
    private final long blockSize;
    private final long modificationTime;
    private final boolean isDir;
    private final Path path;

    private AzureDataLakeFileStatus(
            final Path path,
            final long length,
            final long blockSize,
            final long modificationTime,
            final boolean isDir) {
        checkNotNull(path, "path must not be null");
        checkArgument(length >= 0, "length must be non-negative, got %s", length);
        checkArgument(blockSize >= 0, "blockSize must be non-negative, got %s", blockSize);
        checkArgument(
                modificationTime >= 0,
                "modificationTime must be non-negative, got %s",
                modificationTime);
        this.path = path;
        this.length = length;
        this.blockSize = blockSize;
        this.modificationTime = modificationTime;
        this.isDir = isDir;
    }

    /**
     * Creates a file status for a directory.
     *
     * <p>Directories have zero length and zero block size.
     *
     * @param path the directory path
     * @param modificationTime the last-modified timestamp in milliseconds since epoch
     * @return the file status
     */
    static AzureDataLakeFileStatus forDirectory(final Path path, final long modificationTime) {
        return new AzureDataLakeFileStatus(path, 0L, 0L, modificationTime, true);
    }

    /**
     * Creates a file status for a regular file.
     *
     * @param path the file path
     * @param size the file size in bytes (must be non-negative)
     * @param blockSize the block size in bytes (must be non-negative)
     * @param modificationTime the last-modified timestamp in milliseconds since epoch
     * @return the file status
     */
    static AzureDataLakeFileStatus forFile(
            final Path path, final long size, final long blockSize, final long modificationTime) {
        return new AzureDataLakeFileStatus(path, size, blockSize, modificationTime, false);
    }

    @Override
    public long getLen() {
        return length;
    }

    @Override
    public long getBlockSize() {
        return blockSize;
    }

    /** Azure Data Lake Storage manages replication internally, so this always returns 1. */
    @Override
    public short getReplication() {
        return 1;
    }

    @Override
    public long getModificationTime() {
        return modificationTime;
    }

    @Override
    public long getAccessTime() {
        return DEFAULT_ACCESS_TIME;
    }

    @Override
    public boolean isDir() {
        return isDir;
    }

    @Override
    public Path getPath() {
        return path;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AzureDataLakeFileStatus)) {
            return false;
        }
        final AzureDataLakeFileStatus that = (AzureDataLakeFileStatus) o;
        return length == that.length
                && modificationTime == that.modificationTime
                && isDir == that.isDir
                && blockSize == that.blockSize
                && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, length, blockSize, modificationTime, isDir);
    }

    @Override
    public String toString() {
        return String.format(
                "AzureDataLakeFileStatus{path=%s, length=%d, blockSize=%d, isDir=%b,"
                        + " modTime=%d, accessTime=%d}",
                path, length, blockSize, isDir, modificationTime, DEFAULT_ACCESS_TIME);
    }
}
