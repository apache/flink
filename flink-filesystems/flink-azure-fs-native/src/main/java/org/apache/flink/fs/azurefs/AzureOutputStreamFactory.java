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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.OutputStreamOpener;
import org.apache.flink.util.Preconditions;

import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.PathHttpHeaders;
import com.azure.storage.file.datalake.options.DataLakeFileOutputStreamOptions;

import javax.annotation.concurrent.Immutable;

import java.util.Map;

/** Factory for {@link OutputStreamOpener} and related Azure SDK stream configuration. */
@Immutable
final class AzureOutputStreamFactory {

    private static final String CONTENT_TYPE_OCTET_STREAM = "application/octet-stream";

    private AzureOutputStreamFactory() {}

    /**
     * Creates an {@link OutputStreamOpener} that opens an SDK output stream on the given file.
     *
     * <p>The returned opener captures {@code fsClient}, {@code filePath}, {@code writeRequestSize}
     * and {@code writeMode} in its closure. When invoked, it attaches any metadata from the {@link
     * org.apache.flink.core.fs.WriteContext} to the Azure blob options before opening the stream.
     *
     * @param fsClient the DataLake storage operations abstraction
     * @param filePath relative path of the file within the file system
     * @param writeRequestSize the block size and single-upload threshold in bytes
     * @param writeMode the write mode
     * @return an opener that creates an SDK output stream for the specified file
     */
    static OutputStreamOpener createOpener(
            final DataLakeStorageOperations fsClient,
            final String filePath,
            final long writeRequestSize,
            final WriteMode writeMode) {
        Preconditions.checkNotNull(fsClient, "fsClient");
        Preconditions.checkNotNull(filePath, "filePath");
        Preconditions.checkArgument(writeRequestSize > 0, "writeRequestSize must be positive");
        Preconditions.checkNotNull(writeMode, "writeMode");
        return ctx -> {
            final DataLakeFileOutputStreamOptions options =
                    buildOutputStreamOptions(writeRequestSize, writeMode, ctx.getMetadata());
            return fsClient.getFileClient(filePath).getOutputStream(options);
        };
    }

    /**
     * Builds SDK output stream options for the given write request size, write mode, and metadata.
     *
     * @param writeRequestSize the block size and single-upload threshold in bytes
     * @param writeMode the write mode
     * @param metadata metadata to attach to the blob (e.g., encryption headers); may be empty
     * @return configured output stream options
     */
    @VisibleForTesting
    static DataLakeFileOutputStreamOptions buildOutputStreamOptions(
            final long writeRequestSize,
            final WriteMode writeMode,
            final Map<String, String> metadata) {
        final DataLakeFileOutputStreamOptions options = new DataLakeFileOutputStreamOptions();
        options.setHeaders(new PathHttpHeaders().setContentType(CONTENT_TYPE_OCTET_STREAM));
        options.setParallelTransferOptions(
                new ParallelTransferOptions()
                        .setBlockSizeLong(writeRequestSize)
                        .setMaxSingleUploadSizeLong(writeRequestSize));
        if (writeMode == WriteMode.NO_OVERWRITE) {
            options.setRequestConditions(new DataLakeRequestConditions().setIfNoneMatch("*"));
        }
        if (metadata != null && !metadata.isEmpty()) {
            options.setMetadata(metadata);
        }
        return options;
    }
}
