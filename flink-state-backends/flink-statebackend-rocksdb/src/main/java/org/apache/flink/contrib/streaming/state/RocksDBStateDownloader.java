/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import org.apache.flink.shaded.guava30.com.google.common.collect.Streams;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Help class for downloading RocksDB state files. */
public class RocksDBStateDownloader extends RocksDBStateDataTransfer {
    public RocksDBStateDownloader(int restoringThreadNum) {
        super(restoringThreadNum);
    }

    /**
     * Transfer all state data to the target directory, as specified in the download requests.
     *
     * @param downloadRequests the list of downloads.
     * @throws Exception If anything about the download goes wrong.
     */
    public void transferAllStateDataToDirectory(
            Collection<StateHandleDownloadSpec> downloadRequests,
            CloseableRegistry closeableRegistry)
            throws Exception {
        // We use this closer for fine-grained shutdown of all parallel downloading.
        CloseableRegistry internalCloser = new CloseableRegistry();
        // Make sure we also react to external close signals.
        closeableRegistry.registerCloseable(internalCloser);
        boolean failureCleanupRequired = true;
        List<CompletableFuture<Void>> futures = Collections.emptyList();
        try {
            futures =
                    transferAllStateDataToDirectoryAsync(downloadRequests, internalCloser)
                            .collect(Collectors.toList());
            // Wait until either all futures completed successfully or one failed exceptionally.
            FutureUtils.waitForAll(futures).get();
            // All went well, no failure cleanup required
            failureCleanupRequired = false;
        } catch (ExecutionException e) {
            Throwable throwable = ExceptionUtils.stripExecutionException(e);
            throwable = ExceptionUtils.stripException(throwable, RuntimeException.class);
            if (throwable instanceof IOException) {
                throw (IOException) throwable;
            } else {
                throw new FlinkRuntimeException("Failed to download data for state handles.", e);
            }
        } finally {
            // Unregister and close the internal closer. In a failure case, this should interrupt
            // ongoing downloads.
            closeableRegistry.unregisterCloseable(internalCloser);
            IOUtils.closeQuietly(internalCloser);
            if (failureCleanupRequired) {
                // Cleanup on exception: cancel all tasks and delete the created directories
                futures.forEach(future -> future.cancel(true));
                downloadRequests.stream()
                        .map(StateHandleDownloadSpec::getDownloadDestination)
                        .map(Path::toFile)
                        .forEach(FileUtils::deleteDirectoryQuietly);
            }
        }
    }

    /** Asynchronously runs the specified download requests on executorService. */
    private Stream<CompletableFuture<Void>> transferAllStateDataToDirectoryAsync(
            Collection<StateHandleDownloadSpec> handleWithPaths,
            CloseableRegistry closeableRegistry) {
        return createDownloadRunnables(handleWithPaths, closeableRegistry)
                .map(runnable -> CompletableFuture.runAsync(runnable, executorService));
    }

    /** Creates the {@link Runnable} instances for each files in the download requests. */
    private Stream<Runnable> createDownloadRunnables(
            Collection<StateHandleDownloadSpec> handleWithPaths,
            CloseableRegistry closeableRegistry) {
        return handleWithPaths.stream()
                .flatMap(
                        downloadRequest ->
                                // Take all files from shared and private state.
                                Streams.concat(
                                                downloadRequest.getStateHandle().getSharedState()
                                                        .entrySet().stream(),
                                                downloadRequest.getStateHandle().getPrivateState()
                                                        .entrySet().stream())
                                        .map(
                                                // Create one runnable for each StreamStateHandle
                                                entry -> {
                                                    StateHandleID stateHandleID = entry.getKey();
                                                    StreamStateHandle remoteFileHandle =
                                                            entry.getValue();
                                                    Path downloadDest =
                                                            downloadRequest
                                                                    .getDownloadDestination()
                                                                    .resolve(
                                                                            stateHandleID
                                                                                    .toString());
                                                    return ThrowingRunnable.unchecked(
                                                            () ->
                                                                    downloadDataForStateHandle(
                                                                            downloadDest,
                                                                            remoteFileHandle,
                                                                            closeableRegistry));
                                                }));
    }

    /** Copies the file from a single state handle to the given path. */
    private void downloadDataForStateHandle(
            Path restoreFilePath,
            StreamStateHandle remoteFileHandle,
            CloseableRegistry closeableRegistry)
            throws IOException {

        FSDataInputStream inputStream = null;
        OutputStream outputStream = null;

        try {
            inputStream = remoteFileHandle.openInputStream();
            closeableRegistry.registerCloseable(inputStream);

            Files.createDirectories(restoreFilePath.getParent());
            outputStream = Files.newOutputStream(restoreFilePath);
            closeableRegistry.registerCloseable(outputStream);

            byte[] buffer = new byte[8 * 1024];
            while (true) {
                int numBytes = inputStream.read(buffer);
                if (numBytes == -1) {
                    break;
                }

                outputStream.write(buffer, 0, numBytes);
            }
        } finally {
            if (closeableRegistry.unregisterCloseable(inputStream)) {
                inputStream.close();
            }

            if (closeableRegistry.unregisterCloseable(outputStream)) {
                outputStream.close();
            }
        }
    }
}
