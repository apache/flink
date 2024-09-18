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

package org.apache.flink.state.forst;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.state.forst.fs.ForStFlinkFileSystem;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.CheckedSupplier;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.flink.util.concurrent.Executors.newDirectExecutorService;

/** Data transfer util class for {@link ForStKeyedStateBackend}. */
public class ForStStateDataTransfer implements Closeable {
    private static final int READ_BUFFER_SIZE = 64 * 1024;

    // TODO: Add ConfigOption replace this field after ForSt checkpoint implementation stable
    public static final int DEFAULT_THREAD_NUM = 4;

    protected final ExecutorService executorService;

    private final ForStFlinkFileSystem forStFs;

    public ForStStateDataTransfer(int threadNum) {
        this(threadNum, null);
    }

    public ForStStateDataTransfer(int threadNum, ForStFlinkFileSystem forStFs) {
        this.forStFs = forStFs;
        if (threadNum > 1) {
            executorService =
                    Executors.newFixedThreadPool(
                            threadNum, new ExecutorThreadFactory("Flink-ForStStateDataTransfer"));
        } else {
            executorService = newDirectExecutorService();
        }
    }

    /**
     * Transfer a single file to checkpoint filesystem.
     *
     * @param transferBytes Bytes will be transfer from the head of the file. If < 0, the whole file
     *     will be transferred.
     */
    public HandleAndLocalPath transferFileToCheckpointFs(
            Path file,
            long transferBytes,
            CheckpointStreamFactory checkpointStreamFactory,
            CheckpointedStateScope stateScope,
            CloseableRegistry snapshotCloseableRegistry,
            CloseableRegistry tmpResourcesRegistry)
            throws Exception {

        try {
            return createTransferFuture(
                            file,
                            transferBytes,
                            checkpointStreamFactory,
                            stateScope,
                            snapshotCloseableRegistry,
                            tmpResourcesRegistry)
                    .get();
        } catch (ExecutionException e) {
            throw convertExecutionException(e);
        }
    }

    /** Transfer a batch of files to checkpoint filesystem. */
    public List<HandleAndLocalPath> transferFilesToCheckpointFs(
            List<Path> files,
            CheckpointStreamFactory checkpointStreamFactory,
            CheckpointedStateScope stateScope,
            CloseableRegistry closeableRegistry,
            CloseableRegistry tmpResourcesRegistry)
            throws Exception {

        List<CompletableFuture<HandleAndLocalPath>> futures =
                files.stream()
                        .map(
                                file ->
                                        createTransferFuture(
                                                file,
                                                -1,
                                                checkpointStreamFactory,
                                                stateScope,
                                                closeableRegistry,
                                                tmpResourcesRegistry))
                        .collect(Collectors.toList());

        try {
            List<HandleAndLocalPath> handles = new ArrayList<>(files.size());

            for (CompletableFuture<HandleAndLocalPath> future : futures) {
                handles.add(future.get());
            }

            return handles;
        } catch (ExecutionException e) {
            throw convertExecutionException(e);
        }
    }

    /** Write a file to checkpoint filesystem. */
    public HandleAndLocalPath writeFileToCheckpointFs(
            String filename,
            String fileContent,
            CheckpointStreamFactory checkpointStreamFactory,
            CheckpointedStateScope stateScope,
            CloseableRegistry closeableRegistry,
            CloseableRegistry tmpResourcesRegistry)
            throws IOException {

        CheckpointStateOutputStream outputStream = null;

        try {
            outputStream = checkpointStreamFactory.createCheckpointStateOutputStream(stateScope);
            closeableRegistry.registerCloseable(outputStream);

            byte[] content = fileContent.getBytes(StandardCharsets.UTF_8);

            outputStream.write(content, 0, content.length);

            final StreamStateHandle result;
            if (closeableRegistry.unregisterCloseable(outputStream)) {
                result = outputStream.closeAndGetHandle();
                outputStream = null;
            } else {
                result = null;
            }
            tmpResourcesRegistry.registerCloseable(
                    () -> StateUtil.discardStateObjectQuietly(result));

            return HandleAndLocalPath.of(result, filename);

        } finally {
            if (closeableRegistry.unregisterCloseable(outputStream)) {
                IOUtils.closeQuietly(outputStream);
            }
        }
    }

    private CompletableFuture<HandleAndLocalPath> createTransferFuture(
            Path file,
            long transferBytes,
            CheckpointStreamFactory checkpointStreamFactory,
            CheckpointedStateScope stateScope,
            CloseableRegistry closeableRegistry,
            CloseableRegistry tmpResourcesRegistry) {
        return CompletableFuture.supplyAsync(
                CheckedSupplier.unchecked(
                        () ->
                                transferFile(
                                        file,
                                        transferBytes,
                                        checkpointStreamFactory,
                                        stateScope,
                                        closeableRegistry,
                                        tmpResourcesRegistry)),
                executorService);
    }

    private HandleAndLocalPath transferFile(
            Path filePath,
            long maxTransferBytes,
            CheckpointStreamFactory checkpointStreamFactory,
            CheckpointedStateScope stateScope,
            CloseableRegistry closeableRegistry,
            CloseableRegistry tmpResourcesRegistry)
            throws IOException {

        if (maxTransferBytes < 0) {
            // Means transfer whole file to checkpoint storage.
            maxTransferBytes = Long.MAX_VALUE;

            // TODO: Optimizing transfer with fast duplicate
        }

        InputStream inputStream = null;
        CheckpointStateOutputStream outputStream = null;

        try {
            final byte[] buffer = new byte[READ_BUFFER_SIZE];

            FileSystem sourceFilesystem = forStFs == null ? filePath.getFileSystem() : forStFs;
            inputStream = sourceFilesystem.open(filePath, READ_BUFFER_SIZE);
            closeableRegistry.registerCloseable(inputStream);

            outputStream = checkpointStreamFactory.createCheckpointStateOutputStream(stateScope);
            closeableRegistry.registerCloseable(outputStream);

            while (maxTransferBytes > 0) {
                int maxReadBytes = (int) Math.min(maxTransferBytes, READ_BUFFER_SIZE);
                int readBytes = inputStream.read(buffer, 0, maxReadBytes);

                if (readBytes == -1) {
                    break;
                }

                outputStream.write(buffer, 0, readBytes);

                maxTransferBytes -= readBytes;
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

            return HandleAndLocalPath.of(result, filePath.getName());

        } finally {

            if (closeableRegistry.unregisterCloseable(inputStream)) {
                IOUtils.closeQuietly(inputStream);
            }

            if (closeableRegistry.unregisterCloseable(outputStream)) {
                IOUtils.closeQuietly(outputStream);
            }
        }
    }

    @Override
    public void close() {
        executorService.shutdownNow();
    }

    private Exception convertExecutionException(ExecutionException e) {
        Throwable throwable = ExceptionUtils.stripExecutionException(e);
        throwable = ExceptionUtils.stripException(throwable, RuntimeException.class);
        if (throwable instanceof IOException) {
            return (IOException) throwable;
        } else {
            return new FlinkRuntimeException("Failed to transfer data.", e);
        }
    }
}
