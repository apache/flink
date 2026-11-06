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
import org.apache.flink.core.fs.InputStreamOpener;
import org.apache.flink.util.Preconditions;

import com.azure.storage.file.datalake.models.FileRange;
import com.azure.storage.file.datalake.options.DataLakeFileInputStreamOptions;

import javax.annotation.concurrent.Immutable;

/** Factory for {@link InputStreamOpener} and related Azure SDK stream configuration. */
@Immutable
final class AzureInputStreamFactory {

    private AzureInputStreamFactory() {}

    /**
     * Creates an {@link InputStreamOpener} that opens range requests on the given file.
     *
     * <p>SDK exceptions from the returned opener include the Azure request URL (which contains
     * {@code filePath}), hence no additional path wrapping.
     *
     * @param fsClient the DataLake storage operations abstraction
     * @param filePath relative path of the file within the file system
     * @param blockSize SDK read chunk size in bytes
     * @return an opener that captures {@code fsClient}, {@code filePath} and {@code blockSize} in
     *     its closure
     */
    static InputStreamOpener createOpener(
            final DataLakeStorageOperations fsClient, final String filePath, final int blockSize) {
        Preconditions.checkNotNull(fsClient, "fsClient");
        Preconditions.checkNotNull(filePath, "filePath");
        Preconditions.checkArgument(blockSize > 0, "blockSize must be positive");
        return ctx -> {
            final DataLakeFileInputStreamOptions options =
                    buildInputStreamOptions(ctx.getPos(), blockSize);
            return fsClient.getFileClient(filePath).openInputStream(options);
        };
    }

    /**
     * Builds {@link DataLakeFileInputStreamOptions} for a range request starting at {@code
     * position}.
     *
     * @param position byte offset to start reading from; non-positive values omit the range header
     * @param blockSize SDK read chunk size in bytes
     * @return configured options
     */
    @VisibleForTesting
    static DataLakeFileInputStreamOptions buildInputStreamOptions(
            final long position, final int blockSize) {
        final DataLakeFileInputStreamOptions options = new DataLakeFileInputStreamOptions();
        options.setBlockSize(blockSize);
        if (position > 0) {
            options.setRange(new FileRange(position));
        }
        return options;
    }
}
