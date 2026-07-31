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

package org.apache.flink.state.rocksdb;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.ICloseableRegistry;
import org.apache.flink.core.fs.PathsCopyingFileSystem;
import org.apache.flink.core.fs.PathsCopyingFileSystem.CopyRequest;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static org.apache.flink.util.Preconditions.checkState;

/** Help class for downloading RocksDB state files. */
public class RocksDBStateDownloader implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateDownloader.class);

    private final RocksDBStateDataTransferHelper transfer;

    @VisibleForTesting
    public RocksDBStateDownloader(int restoringThreadNum) {
        this(RocksDBStateDataTransferHelper.forThreadNum(restoringThreadNum));
    }

    public RocksDBStateDownloader(RocksDBStateDataTransferHelper transfer) {
        this.transfer = transfer;
    }

    /**
     * Transfer all state data to the target directory, as specified in the download requests.
     *
     * @param downloadRequests the list of downloads.
     * @throws Exception If anything about the download goes wrong.
     */
    public void transferAllStateDataToDirectory(
            Collection<StateHandleDownloadSpec> downloadRequests,
            ICloseableRegistry closeableRegistry)
            throws Exception {

        // We use this closer for fine-grained shutdown of all parallel downloading.
        CloseableRegistry internalCloser = new CloseableRegistry();
        // Make sure we also react to external close signals.
        closeableRegistry.registerCloseable(internalCloser);
        AtomicReference<Throwable> rawException = new AtomicReference<>();
        try {
            // We have to wait for all futures to be completed, to make sure in
            // case of failure that we will clean up all the files
            FutureUtils.ConjunctFuture<Void> downloadFuture =
                    FutureUtils.completeAll(
                            createDownloadRunnables(downloadRequests, internalCloser).stream()
                                    .map(
                                            runnable ->
                                                    CompletableFuture.runAsync(
                                                            runnable,
                                                            transfer.getExecutorService()))
                                    .collect(Collectors.toList()));

            // Capture the raw CompletionException before get() strips it. get() unwraps
            // CompletionException to its cause (RuntimeException), losing the suppressed list
            // that holds all parallel thread failures. whenComplete fires before get() unblocks.
            downloadFuture.whenComplete((v, t) -> rawException.set(t));

            Exception interruptedException = null;
            while (!downloadFuture.isDone() || downloadFuture.isCompletedExceptionally()) {
                try {
                    downloadFuture.get();
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted while waiting for state download, continue waiting");
                    interruptedException = interruptedException == null ? e : interruptedException;
                }
            }
            if (interruptedException != null) {
                Thread.currentThread().interrupt();
                throw interruptedException;
            }
        } catch (Exception e) {
            downloadRequests.stream()
                    .map(StateHandleDownloadSpec::getDownloadDestination)
                    .map(Path::toFile)
                    .forEach(FileUtils::deleteDirectoryQuietly);
            Throwable raw = rawException.get();
            throw buildDownloadException(raw != null ? raw : e);
        } finally {
            // Unregister and close the internal closer.
            if (closeableRegistry.unregisterCloseable(internalCloser)) {
                IOUtils.closeQuietly(internalCloser);
            }
        }
    }

    private Collection<Runnable> createDownloadRunnables(
            Collection<StateHandleDownloadSpec> downloadRequests,
            CloseableRegistry closeableRegistry)
            throws IOException {
        // We need to support recovery from multiple FileSystems. At least one scenario that it can
        // happen is when:
        // 1. A checkpoint/savepoint is created on FileSystem_1
        // 2. Job terminates
        // 3. Configuration is changed use checkpoint directory using FileSystem_2
        // 4. Job is restarted from checkpoint (1.) using claim mode
        // 5. New incremental checkpoint is created, that can refer to files both from FileSystem_1
        // and FileSystem_2.
        Map<FileSystem.FSKey, List<CopyRequest>> filesSystemsFilesToDownload = new HashMap<>();
        List<Runnable> runnables = new ArrayList<>();

        for (StateHandleDownloadSpec downloadSpec : downloadRequests) {
            for (HandleAndLocalPath handleAndLocalPath : getAllHandles(downloadSpec)) {
                Path downloadDestination =
                        downloadSpec
                                .getDownloadDestination()
                                .resolve(handleAndLocalPath.getLocalPath());
                if (canCopyPaths(handleAndLocalPath)) {
                    org.apache.flink.core.fs.Path remotePath =
                            handleAndLocalPath.getHandle().maybeGetPath().get();
                    long size = handleAndLocalPath.getHandle().getStateSize();
                    FileSystem.FSKey newFSKey = new FileSystem.FSKey(remotePath.toUri());
                    filesSystemsFilesToDownload
                            .computeIfAbsent(newFSKey, fsKey -> new ArrayList<>())
                            .add(
                                    CopyRequest.of(
                                            remotePath,
                                            new org.apache.flink.core.fs.Path(
                                                    downloadDestination.toUri()),
                                            size));
                } else {
                    runnables.add(
                            createDownloadRunnableUsingStreams(
                                    handleAndLocalPath.getHandle(),
                                    downloadDestination,
                                    closeableRegistry));
                }
            }
        }

        for (List<CopyRequest> filesToDownload : filesSystemsFilesToDownload.values()) {
            checkState(!filesToDownload.isEmpty());
            FileSystem srcFileSystem = FileSystem.get(filesToDownload.get(0).getSource().toUri());
            runnables.add(
                    createDownloadRunnableUsingCopyFiles(
                            (PathsCopyingFileSystem) srcFileSystem,
                            filesToDownload,
                            closeableRegistry));
        }

        return runnables;
    }

    private boolean canCopyPaths(HandleAndLocalPath handleAndLocalPath) throws IOException {
        Optional<org.apache.flink.core.fs.Path> remotePath =
                handleAndLocalPath.getHandle().maybeGetPath();
        if (!remotePath.isPresent()) {
            return false;
        }
        return FileSystem.get(remotePath.get().toUri())
                .canCopyPaths(
                        remotePath.get(),
                        new org.apache.flink.core.fs.Path(handleAndLocalPath.getLocalPath()));
    }

    private Iterable<? extends HandleAndLocalPath> getAllHandles(
            StateHandleDownloadSpec downloadSpec) {
        return Stream.concat(
                        downloadSpec.getStateHandle().getSharedState().stream(),
                        downloadSpec.getStateHandle().getPrivateState().stream())
                .collect(Collectors.toList());
    }

    private Runnable createDownloadRunnableUsingCopyFiles(
            PathsCopyingFileSystem fileSystem,
            List<CopyRequest> copyRequests,
            CloseableRegistry closeableRegistry) {
        LOG.debug("Using copy paths for {} of file system [{}]", copyRequests, fileSystem);
        return ThrowingRunnable.unchecked(
                () -> fileSystem.copyFiles(copyRequests, closeableRegistry));
    }

    private Runnable createDownloadRunnableUsingStreams(
            StreamStateHandle remoteFileHandle,
            Path destination,
            CloseableRegistry closeableRegistry) {
        return ThrowingRunnable.unchecked(
                () -> downloadDataForStateHandle(remoteFileHandle, destination, closeableRegistry));
    }

    /** Copies the file from a single state handle to the given path. */
    private void downloadDataForStateHandle(
            StreamStateHandle remoteFileHandle,
            Path restoreFilePath,
            CloseableRegistry closeableRegistry)
            throws IOException {

        if (closeableRegistry.isClosed()) {
            return;
        }

        try {
            FSDataInputStream inputStream = remoteFileHandle.openInputStream();
            closeableRegistry.registerCloseable(inputStream);

            Files.createDirectories(restoreFilePath.getParent());
            OutputStream outputStream = Files.newOutputStream(restoreFilePath, CREATE_NEW);
            closeableRegistry.registerCloseable(outputStream);

            byte[] buffer = new byte[8 * 1024];
            while (true) {
                int numBytes = inputStream.read(buffer);
                if (numBytes == -1) {
                    break;
                }

                outputStream.write(buffer, 0, numBytes);
            }
            closeableRegistry.unregisterAndCloseAll(outputStream, inputStream);
        } catch (Exception ex) {
            // Quickly close all open streams. This also stops all concurrent downloads because they
            // are registered with the same registry.
            IOUtils.closeQuietly(closeableRegistry);
            throw new IOException(ex);
        }
    }

    /**
     * Builds a descriptive {@link IOException} from a potentially cascaded failure across multiple
     * parallel download threads.
     *
     * <p>When one thread fails it closes the shared {@link CloseableRegistry}, causing all other
     * threads to throw a {@code ClosedChannelException} on their local file writes. This method
     * strips the wrapper chain of each collected failure to reach the real {@link IOException},
     * deduplicates by type and message, and returns either the single unique cause directly or a
     * merged exception listing all distinct failures.
     */
    private static IOException buildDownloadException(Throwable rawException) {
        Map<String, Throwable> unique = new LinkedHashMap<>();
        Stream.concat(Stream.of(rawException), Stream.of(rawException.getSuppressed()))
                .map(t -> ExceptionUtils.stripException(t, CompletionException.class))
                .map(t -> ExceptionUtils.stripException(t, RuntimeException.class))
                .forEach(t -> unique.putIfAbsent(t.getClass().getName() + ":" + t.getMessage(), t));

        if (unique.size() == 1) {
            Throwable t = unique.values().iterator().next();
            return t instanceof IOException ? (IOException) t : new IOException(t);
        }

        String summary =
                unique.values().stream()
                        .map(t -> t.getClass().getSimpleName() + ": " + t.getMessage())
                        .collect(Collectors.joining(" | "));
        IOException merged =
                new IOException(
                        unique.size() + " downloads failed with distinct errors: [" + summary + "]",
                        unique.values().iterator().next());
        unique.values().stream().skip(1).forEach(merged::addSuppressed);
        return merged;
    }

    @Override
    public void close() throws IOException {
        this.transfer.close();
    }
}
