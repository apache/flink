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

package org.apache.flink.state.rocksdb;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.ICloseableRegistry;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TestStreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Test class for {@link RocksDBStateDownloader}. */
public class RocksDBStateDownloaderTest extends TestLogger {
    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testWaitForDownloadIfInterrupted()
            throws IOException, InterruptedException, ExecutionException {
        StateHandleDownloadSpec spec =
                new StateHandleDownloadSpec(
                        new IncrementalRemoteKeyedStateHandle(
                                UUID.randomUUID(),
                                KeyGroupRange.EMPTY_KEY_GROUP_RANGE,
                                1L,
                                singletonList(
                                        HandleAndLocalPath.of(
                                                new ByteStreamStateHandle(
                                                        "meta", new byte[] {1, 2, 3, 4}),
                                                "p")),
                                emptyList(),
                                new ByteStreamStateHandle("meta", new byte[] {1, 2, 3, 4}),
                                5L),
                        temporaryFolder.newFolder().toPath().resolve("dst"));
        CompletableFuture<Void> downloaderFuture = new CompletableFuture<>();
        BlockingExecutorService executorService = new BlockingExecutorService();
        Thread downloader = createDownloader(executorService, spec, downloaderFuture);
        downloader.start();
        for (int attempt = 0; attempt < 5; attempt++) {
            downloader.interrupt();
            Thread.sleep(50);
            Assert.assertTrue(
                    "downloader should ignore interrupts while download is in progress",
                    downloader.isAlive());
        }
        executorService.unblock();
        downloaderFuture.get();
        downloader.join();
    }

    private static Thread createDownloader(
            ExecutorService executorService,
            StateHandleDownloadSpec spec,
            CompletableFuture<Void> completionFuture) {
        return new Thread(
                () -> {
                    try (RocksDBStateDownloader rocksDBStateDownloader =
                            new RocksDBStateDownloader(
                                    RocksDBStateDataTransferHelper.forExecutor(executorService))) {
                        try {
                            rocksDBStateDownloader.transferAllStateDataToDirectory(
                                    Collections.singletonList(spec), ICloseableRegistry.NO_OP);
                            completionFuture.complete(null);
                        } catch (Exception e) {
                            if (ExceptionUtils.findThrowable(e, InterruptedException.class)
                                    .isPresent()) {
                                completionFuture.complete(null);
                            } else {
                                completionFuture.completeExceptionally(e);
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /** Test that the exception arose in the thread pool will rethrow to the main thread. */
    @Test
    public void testMultiThreadRestoreThreadPoolExceptionRethrow() {
        SpecifiedException expectedCause =
                new SpecifiedException("throw exception while multi thread restore.");
        StreamStateHandle stateHandle = new ThrowingStateHandle(expectedCause);

        List<HandleAndLocalPath> stateHandles = new ArrayList<>(1);
        stateHandles.add(HandleAndLocalPath.of(stateHandle, "state1"));

        IncrementalRemoteKeyedStateHandle incrementalKeyedStateHandle =
                new IncrementalRemoteKeyedStateHandle(
                        UUID.randomUUID(),
                        KeyGroupRange.EMPTY_KEY_GROUP_RANGE,
                        1,
                        stateHandles,
                        stateHandles,
                        stateHandle);

        try (RocksDBStateDownloader rocksDBStateDownloader = new RocksDBStateDownloader(5)) {
            rocksDBStateDownloader.transferAllStateDataToDirectory(
                    Collections.singletonList(
                            new StateHandleDownloadSpec(
                                    incrementalKeyedStateHandle,
                                    temporaryFolder.newFolder().toPath())),
                    new CloseableRegistry());
            fail();
        } catch (Exception e) {
            assertEquals(expectedCause, e.getCause());
        }
    }

    /** Tests that download files with multi-thread correctly. */
    @Test
    public void testMultiThreadRestoreCorrectly() throws Exception {
        int numRemoteHandles = 3;
        int numSubHandles = 6;
        byte[][][] contents = createContents(numRemoteHandles, numSubHandles);
        List<StateHandleDownloadSpec> downloadRequests = new ArrayList<>(numRemoteHandles);
        for (int i = 0; i < numRemoteHandles; ++i) {
            downloadRequests.add(
                    createDownloadRequestForContent(
                            temporaryFolder.newFolder().toPath(), contents[i], i));
        }

        try (RocksDBStateDownloader rocksDBStateDownloader = new RocksDBStateDownloader(4)) {
            rocksDBStateDownloader.transferAllStateDataToDirectory(
                    downloadRequests, new CloseableRegistry());
        }

        for (int i = 0; i < numRemoteHandles; ++i) {
            StateHandleDownloadSpec downloadRequest = downloadRequests.get(i);
            Path dstPath = downloadRequest.getDownloadDestination();
            Assert.assertTrue(dstPath.toFile().exists());
            for (int j = 0; j < numSubHandles; ++j) {
                assertStateContentEqual(
                        contents[i][j], dstPath.resolve(String.format("sharedState-%d-%d", i, j)));
            }
        }
    }

    /** Tests cleanup on download failures. */
    @Test
    public void testMultiThreadCleanupOnFailure() throws Exception {
        int numRemoteHandles = 3;
        int numSubHandles = 6;
        byte[][][] contents = createContents(numRemoteHandles, numSubHandles);
        List<StateHandleDownloadSpec> downloadRequests = new ArrayList<>(numRemoteHandles);
        for (int i = 0; i < numRemoteHandles; ++i) {
            downloadRequests.add(
                    createDownloadRequestForContent(
                            temporaryFolder.newFolder().toPath(), contents[i], i));
        }

        IncrementalRemoteKeyedStateHandle stateHandle =
                downloadRequests.get(downloadRequests.size() - 1).getStateHandle();

        // Add a state handle that induces an exception
        stateHandle
                .getSharedState()
                .add(
                        HandleAndLocalPath.of(
                                new ThrowingStateHandle(new IOException("Test exception.")),
                                "error-handle"));

        CloseableRegistry closeableRegistry = new CloseableRegistry();
        try (RocksDBStateDownloader rocksDBStateDownloader = new RocksDBStateDownloader(5)) {
            rocksDBStateDownloader.transferAllStateDataToDirectory(
                    downloadRequests, closeableRegistry);
            fail("Exception is expected");
        } catch (IOException ignore) {
        }

        // Check that all download directories have been deleted
        for (StateHandleDownloadSpec downloadRequest : downloadRequests) {
            Assert.assertFalse(downloadRequest.getDownloadDestination().toFile().exists());
        }
        // The passed in closable registry should not be closed by us on failure.
        Assert.assertFalse(closeableRegistry.isClosed());
    }

    private void assertStateContentEqual(byte[] expected, Path path) throws IOException {
        byte[] actual = Files.readAllBytes(Paths.get(path.toUri()));
        assertArrayEquals(expected, actual);
    }

    private static class SpecifiedException extends IOException {
        SpecifiedException(String message) {
            super(message);
        }
    }

    private static class ThrowingStateHandle implements TestStreamStateHandle {
        private static final long serialVersionUID = -2102069659550694805L;

        private final IOException expectedException;

        private ThrowingStateHandle(IOException expectedException) {
            this.expectedException = expectedException;
        }

        @Override
        public FSDataInputStream openInputStream() throws IOException {
            throw expectedException;
        }

        @Override
        public Optional<byte[]> asBytesIfInMemory() {
            return Optional.empty();
        }

        @Override
        public void discardState() {}

        @Override
        public long getStateSize() {
            return 0;
        }
    }

    private byte[][][] createContents(int numRemoteHandles, int numSubHandles) {
        Random random = new Random();
        byte[][][] contents = new byte[numRemoteHandles][numSubHandles][];
        for (int i = 0; i < numRemoteHandles; ++i) {
            for (int j = 0; j < numSubHandles; ++j) {
                contents[i][j] = new byte[random.nextInt(100000) + 1];
                random.nextBytes(contents[i][j]);
            }
        }
        return contents;
    }

    private StateHandleDownloadSpec createDownloadRequestForContent(
            Path dstPath, byte[][] content, int remoteHandleId) {
        int numSubHandles = content.length;
        List<StreamStateHandle> handles = new ArrayList<>(numSubHandles);
        for (int i = 0; i < numSubHandles; ++i) {
            handles.add(
                    new ByteStreamStateHandle(
                            String.format("state-%d-%d", remoteHandleId, i), content[i]));
        }

        List<HandleAndLocalPath> sharedStates = new ArrayList<>(numSubHandles);
        List<HandleAndLocalPath> privateStates = new ArrayList<>(numSubHandles);
        for (int i = 0; i < numSubHandles; ++i) {
            sharedStates.add(
                    HandleAndLocalPath.of(
                            handles.get(i), String.format("sharedState-%d-%d", remoteHandleId, i)));
            privateStates.add(
                    HandleAndLocalPath.of(
                            handles.get(i),
                            String.format("privateState-%d-%d", remoteHandleId, i)));
        }

        IncrementalRemoteKeyedStateHandle incrementalKeyedStateHandle =
                new IncrementalRemoteKeyedStateHandle(
                        UUID.randomUUID(),
                        KeyGroupRange.of(0, 1),
                        1,
                        sharedStates,
                        privateStates,
                        handles.get(0));

        return new StateHandleDownloadSpec(incrementalKeyedStateHandle, dstPath);
    }

    private static class BlockingExecutorService implements ExecutorService {
        private final CompletableFuture<Void> unblockFuture = new CompletableFuture<>();
        private final ExecutorService delegate;

        private BlockingExecutorService() {
            delegate = Executors.newSingleThreadExecutor();
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Nonnull
        @Override
        public List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }

        @Nonnull
        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return delegate.submit(wrap(task));
        }

        @Nonnull
        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            return delegate.submit(wrap(task), result);
        }

        @Nonnull
        @Override
        public Future<?> submit(Runnable task) {
            return delegate.submit(wrap(task));
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
                throws InterruptedException {
            return delegate.invokeAll(wrap(tasks));
        }

        @Nonnull
        @Override
        public <T> List<Future<T>> invokeAll(
                Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                throws InterruptedException {
            return delegate.invokeAll(wrap(tasks), timeout, unit);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
                throws InterruptedException, ExecutionException {
            return delegate.invokeAny(wrap(tasks));
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            return delegate.invokeAny(wrap(tasks), timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(wrap(command));
        }

        private <T> Callable<T> wrap(Callable<T> task) {
            return () -> {
                T result = task.call();
                unblockFuture.join();
                return result;
            };
        }

        private Runnable wrap(Runnable task) {
            return () -> {
                try {
                    unblockFuture.join();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                task.run();
            };
        }

        private <T> List<Callable<T>> wrap(Collection<? extends Callable<T>> tasks) {
            return tasks.stream().map(this::wrap).collect(Collectors.toList());
        }

        public void unblock() {
            this.unblockFuture.complete(null);
        }
    }
}
