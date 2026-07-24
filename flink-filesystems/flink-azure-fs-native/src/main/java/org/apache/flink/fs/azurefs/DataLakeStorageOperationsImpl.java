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

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
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

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Production implementation of {@link DataLakeStorageOperations} backed by Azure SDK's {@link
 * DataLakeFileSystemClient}.
 */
@ThreadSafe
final class DataLakeStorageOperationsImpl implements DataLakeStorageOperations {

    private final DataLakeFileSystemClient fsClient;

    DataLakeStorageOperationsImpl(final DataLakeFileSystemClient fsClient) {
        this.fsClient = checkNotNull(fsClient, "fsClient");
    }

    @Override
    public FileClient getFileClient(final String path) {
        final DataLakeFileClient fileClient = fsClient.getFileClient(path);
        return new FileClient() {
            @Override
            public PathProperties getProperties() throws DataLakeStorageException {
                return fileClient.getProperties();
            }

            @Override
            public void delete() throws DataLakeStorageException {
                fileClient.delete();
            }

            @Override
            public void rename(final String fileSystem, final String destinationPath)
                    throws DataLakeStorageException {
                fileClient.rename(fileSystem, destinationPath);
            }

            @Override
            public OutputStream getOutputStream(final DataLakeFileOutputStreamOptions options)
                    throws DataLakeStorageException {
                return fileClient.getOutputStream(options);
            }

            @Override
            public InputStream openInputStream(final DataLakeFileInputStreamOptions options)
                    throws DataLakeStorageException {
                return fileClient.openInputStream(options).getInputStream();
            }
        };
    }

    @Override
    public DirectoryClient getDirectoryClient(final String path) {
        final DataLakeDirectoryClient directoryClient = fsClient.getDirectoryClient(path);
        return new DirectoryClient() {
            @Override
            public void delete() throws DataLakeStorageException {
                directoryClient.delete();
            }

            @Override
            public void deleteRecursively() throws DataLakeStorageException {
                directoryClient.deleteRecursively();
            }

            @Override
            public void createIfNotExists() throws DataLakeStorageException {
                directoryClient.createIfNotExists();
            }

            @Override
            public void rename(final String fileSystem, final String destinationPath)
                    throws DataLakeStorageException {
                directoryClient.rename(fileSystem, destinationPath);
            }
        };
    }

    @Override
    public Iterable<PathItem> listPaths(
            final ListPathsOptions options, @Nullable final Duration timeout)
            throws DataLakeStorageException {
        return fsClient.listPaths(options, timeout);
    }
}
