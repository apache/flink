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
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.function.CheckedSupplier;
import org.apache.flink.util.function.ThrowingRunnable;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import static org.apache.flink.runtime.concurrent.Executors.newDirectExecutorService;

/**
 * Data transfer utils for {@link RocksDBKeyedStateBackend}.
 */
public class RocksDbStateDataTransfer {
	private static final int READ_BUFFER_SIZE = 16 * 1024;

	/**
	 * Transfer all state data to the target directory using specified number of threads.
	 *
	 * @param restoreStateHandle Handles used to retrieve the state data.
	 * @param dest The target directory which the state data will be stored.
	 * @param restoringThreadNum The number of threads used to download the state data.
	 * @param closeableRegistry Which all the inputStream/outputStream will be registered and unregistered.
	 *
	 * @throws Exception Thrown if can not transfer all the state data.
	 */
	public static void transferAllStateDataToDirectory(
		IncrementalKeyedStateHandle restoreStateHandle,
		Path dest,
		int restoringThreadNum,
		CloseableRegistry closeableRegistry) throws Exception {

		final Map<StateHandleID, StreamStateHandle> sstFiles =
			restoreStateHandle.getSharedState();
		final Map<StateHandleID, StreamStateHandle> miscFiles =
			restoreStateHandle.getPrivateState();

		downloadDataForAllStateHandles(sstFiles, dest, restoringThreadNum, closeableRegistry);
		downloadDataForAllStateHandles(miscFiles, dest, restoringThreadNum, closeableRegistry);
	}

	/**
	 * Upload all the files to checkpoint fileSystem using specified number of threads.
	 *
	 * @param files The files will be uploaded to checkpoint filesystem.
	 * @param numberOfSnapshottingThreads The number of threads used to upload the files.
	 * @param checkpointStreamFactory The checkpoint streamFactory used to create outputstream.
	 * @param closeableRegistry
	 *
	 * @throws Exception Thrown if can not upload all the files.
	 */
	public static Map<StateHandleID, StreamStateHandle> uploadFilesToCheckpointFs(
		@Nonnull Map<StateHandleID, Path> files,
		int numberOfSnapshottingThreads,
		CheckpointStreamFactory checkpointStreamFactory,
		CloseableRegistry closeableRegistry) throws Exception {

		Map<StateHandleID, StreamStateHandle> handles = new HashMap<>();

		ExecutorService executorService = createExecutorService(numberOfSnapshottingThreads);

		Map<StateHandleID, CompletableFuture<StreamStateHandle>> futures =
			createUploadFutures(
				files,
				checkpointStreamFactory,
				closeableRegistry,
				executorService);

		try {
			FutureUtils.waitForAll(futures.values()).get();

			for (Map.Entry<StateHandleID, CompletableFuture<StreamStateHandle>> entry : futures.entrySet()) {
				handles.put(entry.getKey(), entry.getValue().get());
			}
		} catch (ExecutionException e) {
			Throwable throwable = ExceptionUtils.stripExecutionException(e);
			throwable = ExceptionUtils.stripException(throwable, RuntimeException.class);
			if (throwable instanceof IOException) {
				throw (IOException) throwable;
			} else {
				throw new FlinkRuntimeException("Failed to download data for state handles.", e);
			}
		} finally {
			executorService.shutdownNow();
		}
		return handles;
	}

	private static Map<StateHandleID, CompletableFuture<StreamStateHandle>> createUploadFutures(
		Map<StateHandleID, Path> files,
		CheckpointStreamFactory checkpointStreamFactory,
		CloseableRegistry closeableRegistry,
		ExecutorService executorService) {
		Map<StateHandleID, CompletableFuture<StreamStateHandle>> futures = new HashMap<>(files.size());

		for (Map.Entry<StateHandleID, Path> entry : files.entrySet()) {
			final Supplier<StreamStateHandle> supplier =
				CheckedSupplier.unchecked(() -> uploadLocalFileToCheckpointFs(entry.getValue(), checkpointStreamFactory, closeableRegistry));
			futures.put(entry.getKey(), CompletableFuture.supplyAsync(supplier, executorService));
		}

		return futures;
	}

	private static StreamStateHandle uploadLocalFileToCheckpointFs(
		Path filePath,
		CheckpointStreamFactory checkpointStreamFactory,
		CloseableRegistry closeableRegistry) throws IOException {
		FSDataInputStream inputStream = null;
		CheckpointStreamFactory.CheckpointStateOutputStream outputStream = null;

		try {
			final byte[] buffer = new byte[READ_BUFFER_SIZE];

			FileSystem backupFileSystem = filePath.getFileSystem();
			inputStream = backupFileSystem.open(filePath);
			closeableRegistry.registerCloseable(inputStream);

			outputStream = checkpointStreamFactory
				.createCheckpointStateOutputStream(CheckpointedStateScope.SHARED);
			closeableRegistry.registerCloseable(outputStream);

			while (true) {
				int numBytes = inputStream.read(buffer);

				if (numBytes == -1) {
					break;
				}

				outputStream.write(buffer, 0, numBytes);
			}

			StreamStateHandle result = null;
			if (closeableRegistry.unregisterCloseable(outputStream)) {
				result = outputStream.closeAndGetHandle();
				outputStream = null;
			}
			return result;

		} finally {

			if (closeableRegistry.unregisterCloseable(inputStream)) {
				IOUtils.closeQuietly(inputStream);
			}

			if (closeableRegistry.unregisterCloseable(outputStream)) {
				IOUtils.closeQuietly(outputStream);
			}
		}
	}

	/**
	 * Copies all the files from the given stream state handles to the given path, renaming the files w.r.t. their
	 * {@link StateHandleID}.
	 */
	private static void downloadDataForAllStateHandles(
		Map<StateHandleID, StreamStateHandle> stateHandleMap,
		Path restoreInstancePath,
		int restoringThreadNum,
		CloseableRegistry closeableRegistry) throws Exception {

		final ExecutorService executorService = createExecutorService(restoringThreadNum);

		try {
			List<Runnable> runnables = createDownloadRunnables(stateHandleMap, restoreInstancePath, closeableRegistry);
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
		} finally {
			executorService.shutdownNow();
		}
	}

	private static ExecutorService createExecutorService(int threadNum) {
		if (threadNum > 1) {
			return Executors.newFixedThreadPool(threadNum);
		} else {
			return newDirectExecutorService();
		}
	}

	private static List<Runnable> createDownloadRunnables(
		Map<StateHandleID, StreamStateHandle> stateHandleMap,
		Path restoreInstancePath,
		CloseableRegistry closeableRegistry) {
		List<Runnable> runnables = new ArrayList<>(stateHandleMap.size());
		for (Map.Entry<StateHandleID, StreamStateHandle> entry : stateHandleMap.entrySet()) {
			StateHandleID stateHandleID = entry.getKey();
			StreamStateHandle remoteFileHandle = entry.getValue();

			Path path = new Path(restoreInstancePath, stateHandleID.toString());

			runnables.add(ThrowingRunnable.unchecked(
				() -> downloadDataForStateHandle(path, remoteFileHandle, closeableRegistry)));
		}
		return runnables;
	}

	/**
	 * Copies the file from a single state handle to the given path.
	 */
	private static void downloadDataForStateHandle(
		Path restoreFilePath,
		StreamStateHandle remoteFileHandle,
		CloseableRegistry closeableRegistry) throws IOException {

		FSDataInputStream inputStream = null;
		FSDataOutputStream outputStream = null;

		try {
			FileSystem restoreFileSystem = restoreFilePath.getFileSystem();
			inputStream = remoteFileHandle.openInputStream();
			closeableRegistry.registerCloseable(inputStream);

			outputStream = restoreFileSystem.create(restoreFilePath, FileSystem.WriteMode.OVERWRITE);
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
