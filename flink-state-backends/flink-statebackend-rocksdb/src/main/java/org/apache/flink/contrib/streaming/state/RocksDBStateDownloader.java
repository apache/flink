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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/** Help class for downloading RocksDB state files. */
public class RocksDBStateDownloader extends RocksDBStateDataTransfer {
    public RocksDBStateDownloader(int restoringThreadNum) {
        super(restoringThreadNum);
    }

    /**
     * Transfer all state data to the target directory using specified number of threads.
     *
     * @param restoreStateHandle Handles used to retrieve the state data.
     * @param dest The target directory which the state data will be stored.
     * @throws Exception Thrown if can not transfer all the state data.
     */
    public void transferAllStateDataToDirectory(
            IncrementalRemoteKeyedStateHandle restoreStateHandle,
            Path dest,
            CloseableRegistry closeableRegistry)
            throws Exception {

        final Map<StateHandleID, StreamStateHandle> sstFiles = restoreStateHandle.getSharedState();
        final Map<StateHandleID, StreamStateHandle> miscFiles =
                restoreStateHandle.getPrivateState();

        downloadDataForAllStateHandles(sstFiles, dest, closeableRegistry);
        downloadDataForAllStateHandles(miscFiles, dest, closeableRegistry);
    }

    /**
     * Copies all the files from the given stream state handles to the given path, renaming the
     * files w.r.t. their {@link StateHandleID}.
     */
    private void downloadDataForAllStateHandles(
            Map<StateHandleID, StreamStateHandle> stateHandleMap,
            Path restoreInstancePath,
            CloseableRegistry closeableRegistry)
            throws Exception {

        try {
            List<Runnable> runnables =
                    createDownloadRunnables(stateHandleMap, restoreInstancePath, closeableRegistry);
            List<CompletableFuture<Void>> futures = new ArrayList<>(runnables.size());
            for (Runnable runnable : runnables) {
                futures.add(CompletableFuture.runAsync(runnable, executorService));
            }
            FutureUtils.waitForAll(futures).get();
        } catch (ExecutionException e) {
            Throwable throwable = ExceptionUtils.stripExecutionException(e);
            throwable = ExceptionUtils.stripException(throwable, RuntimeException.class);
            if (throwable instanceof IOException) {
                throw (IOException) throwable;
            } else {
                throw new FlinkRuntimeException("Failed to download data for state handles.", e);
            }
        }
    }

    private List<Runnable> createDownloadRunnables(
            Map<StateHandleID, StreamStateHandle> stateHandleMap,
            Path restoreInstancePath,
            CloseableRegistry closeableRegistry) {
        List<Runnable> runnables = new ArrayList<>(stateHandleMap.size());
        for (Map.Entry<StateHandleID, StreamStateHandle> entry : stateHandleMap.entrySet()) {
            StateHandleID stateHandleID = entry.getKey();
            StreamStateHandle remoteFileHandle = entry.getValue();

            Path path = restoreInstancePath.resolve(stateHandleID.toString());

            runnables.add(
                    ThrowingRunnable.unchecked(
                            () ->
                                    downloadDataForStateHandle(
                                            path, remoteFileHandle, closeableRegistry)));
        }
        return runnables;
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
