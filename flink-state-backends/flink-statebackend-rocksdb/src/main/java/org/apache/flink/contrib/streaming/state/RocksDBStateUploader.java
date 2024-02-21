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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.CheckedSupplier;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/** Help class for uploading RocksDB state files. */
public class RocksDBStateUploader extends RocksDBStateDataTransfer {
    private static final int READ_BUFFER_SIZE = 16 * 1024;

    public RocksDBStateUploader(int numberOfSnapshottingThreads) {
        super(numberOfSnapshottingThreads);
    }

    /**
     * Upload all the files to checkpoint fileSystem using specified number of threads.
     *
     * @param files The files will be uploaded to checkpoint filesystem.
     * @param checkpointStreamFactory The checkpoint streamFactory used to create outputstream.
     * @param stateScope
     * @throws Exception Thrown if can not upload all the files.
     */
    public List<HandleAndLocalPath> uploadFilesToCheckpointFs(
            @Nonnull List<Path> files,
            CheckpointStreamFactory checkpointStreamFactory,
            CheckpointedStateScope stateScope,
            CloseableRegistry closeableRegistry,
            CloseableRegistry tmpResourcesRegistry)
            throws Exception {

        List<CompletableFuture<HandleAndLocalPath>> futures =
                createUploadFutures(
                        files,
                        checkpointStreamFactory,
                        stateScope,
                        closeableRegistry,
                        tmpResourcesRegistry);

        List<HandleAndLocalPath> handles = new ArrayList<>(files.size());

        try {
            FutureUtils.waitForAll(futures).get();

            for (CompletableFuture<HandleAndLocalPath> future : futures) {
                handles.add(future.get());
            }
        } catch (ExecutionException e) {
            Throwable throwable = ExceptionUtils.stripExecutionException(e);
            throwable = ExceptionUtils.stripException(throwable, RuntimeException.class);
            if (throwable instanceof IOException) {
                throw (IOException) throwable;
            } else {
                throw new FlinkRuntimeException("Failed to upload data for state handles.", e);
            }
        }

        return handles;
    }

    private List<CompletableFuture<HandleAndLocalPath>> createUploadFutures(
            List<Path> files,
            CheckpointStreamFactory checkpointStreamFactory,
            CheckpointedStateScope stateScope,
            CloseableRegistry closeableRegistry,
            CloseableRegistry tmpResourcesRegistry) {
        return files.stream()
                .map(
                        e ->
                                CompletableFuture.supplyAsync(
                                        CheckedSupplier.unchecked(
                                                () ->
                                                        uploadLocalFileToCheckpointFs(
                                                                e,
                                                                checkpointStreamFactory,
                                                                stateScope,
                                                                closeableRegistry,
                                                                tmpResourcesRegistry)),
                                        executorService))
                .collect(Collectors.toList());
    }

    private HandleAndLocalPath uploadLocalFileToCheckpointFs(
            Path filePath,
            CheckpointStreamFactory checkpointStreamFactory,
            CheckpointedStateScope stateScope,
            CloseableRegistry closeableRegistry,
            CloseableRegistry tmpResourcesRegistry)
            throws IOException {

        InputStream inputStream = null;
        CheckpointStateOutputStream outputStream = null;

        try {
            final byte[] buffer = new byte[READ_BUFFER_SIZE];

            inputStream = Files.newInputStream(filePath);
            closeableRegistry.registerCloseable(inputStream);

            outputStream = checkpointStreamFactory.createCheckpointStateOutputStream(stateScope);
            closeableRegistry.registerCloseable(outputStream);

            while (true) {
                int numBytes = inputStream.read(buffer);

                if (numBytes == -1) {
                    break;
                }

                outputStream.write(buffer, 0, numBytes);
            }

            final StreamStateHandle result;
            if (closeableRegistry.unregisterCloseable(outputStream)) {
                result = outputStream.closeAndGetHandle();
                outputStream = null;
            } else {
                result = null;
            }
            tmpResourcesRegistry.registerCloseable(
                    () -> StateUtil.discardStateObjectQuietly(result));
            return HandleAndLocalPath.of(result, filePath.getFileName().toString());

        } finally {

            if (closeableRegistry.unregisterCloseable(inputStream)) {
                IOUtils.closeQuietly(inputStream);
            }

            if (closeableRegistry.unregisterCloseable(outputStream)) {
                IOUtils.closeQuietly(outputStream);
            }
        }
    }
}
