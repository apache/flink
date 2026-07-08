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

import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.PathProperties;
import com.azure.storage.file.datalake.options.DataLakeFileInputStreamOptions;
import com.azure.storage.file.datalake.options.DataLakeFileOutputStreamOptions;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;

/**
 * Abstraction over Azure DataLake storage operations for testability.
 *
 * <p>Mirrors the Azure SDK's client structure: {@code getFileClient(path).xxx()} and {@code
 * getDirectoryClient(path).xxx()}.
 *
 * <p>Implementations should be thread-safe — {@link AzureDataLakeFileSystem} shares a single
 * instance across threads.
 */
@ThreadSafe
interface DataLakeStorageOperations {

    /** Client for file operations on a specific path. */
    interface FileClient {
        /** Returns the properties (size, modification time, metadata) of the file. */
        PathProperties getProperties() throws DataLakeStorageException;

        /** Deletes the file. */
        void delete() throws DataLakeStorageException;

        /**
         * Renames the file to a new path within the given file system.
         *
         * @param fileSystem the destination file system (container) name
         * @param destinationPath the new path for the file
         */
        void rename(String fileSystem, String destinationPath) throws DataLakeStorageException;

        /**
         * Opens an output stream for writing to the file.
         *
         * @param options stream options (block size, request conditions, metadata)
         * @return an output stream for writing
         */
        OutputStream getOutputStream(DataLakeFileOutputStreamOptions options)
                throws DataLakeStorageException;

        /**
         * Opens an input stream for reading from the file.
         *
         * @param options stream options (range, block size)
         * @return an input stream for reading
         */
        InputStream openInputStream(DataLakeFileInputStreamOptions options)
                throws DataLakeStorageException;
    }

    /** Client for directory operations on a specific path. */
    interface DirectoryClient {
        /** Deletes the directory if it is empty. */
        void delete() throws DataLakeStorageException;

        /** Deletes the directory and all its contents recursively. */
        void deleteRecursively() throws DataLakeStorageException;

        /** Creates the directory if it does not already exist. */
        void createIfNotExists() throws DataLakeStorageException;

        /**
         * Renames the directory to a new path within the given file system.
         *
         * @param fileSystem the destination file system (container) name
         * @param destinationPath the new path for the directory
         */
        void rename(String fileSystem, String destinationPath) throws DataLakeStorageException;
    }

    /** Returns a client for file operations on the specified path. */
    FileClient getFileClient(String path);

    /** Returns a client for directory operations on the specified path. */
    DirectoryClient getDirectoryClient(String path);

    /**
     * Lists paths under the specified options.
     *
     * <p>The returned iterable may throw {@link DataLakeStorageException} during iteration (e.g.,
     * on network errors during pagination).
     *
     * @param options listing options (path, recursive flag)
     * @param timeout optional timeout for the operation
     * @return an iterable of path items matching the listing criteria
     */
    Iterable<PathItem> listPaths(ListPathsOptions options, @Nullable Duration timeout)
            throws DataLakeStorageException;
}
