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

package org.apache.flink.state.forst.datatransfer;

import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.state.forst.ForStKeyedStateBackend;
import org.apache.flink.state.forst.ForStPathContainer;
import org.apache.flink.state.forst.StateHandleTransferSpec;
import org.apache.flink.state.forst.fs.ForStFlinkFileSystem;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.CheckedSupplier;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.concurrent.Executors.newDirectExecutorService;

/** Data transfer util class for {@link ForStKeyedStateBackend}. */
public class ForStStateDataTransfer implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ForStStateDataTransfer.class);

    // TODO: Add ConfigOption replace this field after ForSt checkpoint implementation stable
    public static final int DEFAULT_THREAD_NUM = 4;

    protected final ExecutorService executorService;

    @Nullable private final ForStFlinkFileSystem forStFs;

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
            SnapshotType.SharingFilesStrategy sharingFilesStrategy,
            Path file,
            long transferBytes,
            CheckpointStreamFactory checkpointStreamFactory,
            CheckpointedStateScope stateScope,
            CloseableRegistry snapshotCloseableRegistry,
            CloseableRegistry tmpResourcesRegistry,
            boolean forceCopy)
            throws Exception {

        try {
            DataTransferStrategy strategy =
                    DataTransferStrategyBuilder.buildForSnapshot(
                            sharingFilesStrategy, forStFs, checkpointStreamFactory, forceCopy);
            return createTransferFuture(
                            strategy,
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
            SnapshotType.SharingFilesStrategy sharingFilesStrategy,
            List<Path> files,
            CheckpointStreamFactory checkpointStreamFactory,
            CheckpointedStateScope stateScope,
            CloseableRegistry closeableRegistry,
            CloseableRegistry tmpResourcesRegistry,
            boolean forceCopy)
            throws Exception {
        DataTransferStrategy strategy =
                DataTransferStrategyBuilder.buildForSnapshot(
                        sharingFilesStrategy, forStFs, checkpointStreamFactory, forceCopy);
        List<CompletableFuture<HandleAndLocalPath>> futures =
                files.stream()
                        .map(
                                file ->
                                        createTransferFuture(
                                                strategy,
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
            DataTransferStrategy strategy,
            Path file,
            long transferBytes,
            CheckpointStreamFactory checkpointStreamFactory,
            CheckpointedStateScope stateScope,
            CloseableRegistry closeableRegistry,
            CloseableRegistry tmpResourcesRegistry) {
        return CompletableFuture.supplyAsync(
                CheckedSupplier.unchecked(
                        () ->
                                strategy.transferToCheckpoint(
                                        file,
                                        transferBytes,
                                        checkpointStreamFactory,
                                        stateScope,
                                        closeableRegistry,
                                        tmpResourcesRegistry)),
                executorService);
    }

    private FileSystem getDbFileSystem() {
        return forStFs != null ? forStFs : FileSystem.getLocalFileSystem();
    }

    /**
     * Transfer all state data to the target directory, as specified in the transfer requests.
     *
     * @param transferSpecs the list of transfers.
     * @throws Exception If anything about the transfer goes wrong.
     */
    public void transferAllStateDataToDirectory(
            ForStPathContainer forStPathContainer,
            Collection<StateHandleTransferSpec> transferSpecs,
            CloseableRegistry closeableRegistry,
            RecoveryClaimMode recoveryClaimMode)
            throws Exception {

        // We use this closer for fine-grained shutdown of all parallel transferring.
        CloseableRegistry internalCloser = new CloseableRegistry();
        // Make sure we also react to external close signals.
        closeableRegistry.registerCloseable(internalCloser);

        try {
            List<CompletableFuture<Void>> futures =
                    transferAllStateDataToDirectoryAsync(
                                    forStPathContainer,
                                    transferSpecs,
                                    internalCloser,
                                    recoveryClaimMode)
                            .collect(Collectors.toList());

            // Wait until either all futures completed successfully or one failed exceptionally.
            FutureUtils.completeAll(futures).get();

        } catch (ExecutionException e) {

            // Delete the transfer destination quietly.
            transferSpecs.stream()
                    .map(StateHandleTransferSpec::getTransferDestination)
                    .forEach(
                            dir -> {
                                try {
                                    getDbFileSystem().delete(dir, true);
                                } catch (IOException ignored) {
                                    LOG.warn("Failed to delete transfer destination.", ignored);
                                }
                            });

            throw convertExecutionException(e);

        } finally {
            // Unregister and close the internal closer.
            if (closeableRegistry.unregisterCloseable(internalCloser)) {
                IOUtils.closeQuietly(internalCloser);
            }
        }
    }

    /** Asynchronously runs the specified transfer requests on executorService. */
    private Stream<CompletableFuture<Void>> transferAllStateDataToDirectoryAsync(
            ForStPathContainer forStPathContainer,
            Collection<StateHandleTransferSpec> transferSpecs,
            CloseableRegistry closeableRegistry,
            RecoveryClaimMode recoveryClaimMode) {
        DataTransferStrategy strategy =
                DataTransferStrategyBuilder.buildForRestore(
                        forStFs, forStPathContainer, transferSpecs, recoveryClaimMode);

        return transferSpecs.stream()
                .flatMap(
                        spec ->
                                // Take all files from shared and private state.
                                Stream.concat(
                                                spec.getStateHandle().getSharedState().stream(),
                                                spec.getStateHandle().getPrivateState().stream())
                                        .map(
                                                // Create one runnable for each StreamStateHandle
                                                entry -> {
                                                    String localPath = entry.getLocalPath();
                                                    StreamStateHandle sourceHandle =
                                                            entry.getHandle();
                                                    Path targetPath =
                                                            new Path(
                                                                    spec.getTransferDestination(),
                                                                    localPath);
                                                    return ThrowingRunnable.unchecked(
                                                            () ->
                                                                    strategy.transferFromCheckpoint(
                                                                            sourceHandle,
                                                                            targetPath,
                                                                            closeableRegistry));
                                                }))
                .map(runnable -> CompletableFuture.runAsync(runnable, executorService));
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
