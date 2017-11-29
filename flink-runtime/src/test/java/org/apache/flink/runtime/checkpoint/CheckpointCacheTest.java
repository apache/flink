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
package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Tests for CheckpointCache
 */
public class CheckpointCacheTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void testCommitCache() throws Exception {
		File tmpFolder = tmp.newFolder();
		final CheckpointCache cache = new CheckpointCache(new JobID(), tmpFolder.getAbsolutePath(), 10000, 10000, mock(CheckpointCacheManager.class), Executors.directExecutor());

		StateHandleID handleID1 = new StateHandleID("handle1");
		StateHandleID handleID2 = new StateHandleID("handle2");

		Assert.assertEquals(cache.getPendingCheckpointCacheSize(), 0);

		cache.registerCacheEntry(1, handleID1, new FileStateHandle(new Path(tmp.newFile().getAbsolutePath()), 0L));
		cache.registerCacheEntry(1, handleID2, new FileStateHandle(new Path(tmp.newFile().getAbsolutePath()), 0L));

		Assert.assertEquals(cache.getPendingCheckpointCacheSize(), 1);

		cache.commitCache(1);

		Assert.assertEquals(cache.getPendingCheckpointCacheSize(), 0);
		Assert.assertEquals(cache.getCompletedCheckpointCacheSize(), 1);

		cache.release();
	}

	@Test
	public void testOpenInputStream() throws Exception {
		File tmpFolder = tmp.newFolder();
		final CheckpointCache cache = new CheckpointCache(new JobID(), tmpFolder.getAbsolutePath(), 10000, 10000, mock(CheckpointCacheManager.class), Executors.directExecutor());

		StateHandleID handleID1 = new StateHandleID("handle1");
		cache.registerCacheEntry(1, handleID1, new FileStateHandle(new Path("handle1"), 0L));

		File handle2File = tmp.newFile("handle2");

		final String testStr = "test str";

		FileOutputStream outputStream = new FileOutputStream(handle2File);
		outputStream.write(testStr.getBytes(), 0, testStr.getBytes().length);
		outputStream.close();

		StateHandleID handleID2 = new StateHandleID(handle2File.getAbsolutePath());
		cache.registerCacheEntry(1, handleID2, new FileStateHandle(new Path(handle2File.getAbsolutePath()), 0L));

		cache.commitCache(1);

		Assert.assertNull(cache.openInputStream(handleID1));

		//valid content
		FSDataInputStream inputStream = cache.openInputStream(handleID2);
		byte[] readBytes = new byte[testStr.length()];
		inputStream.read(readBytes);
		Assert.assertEquals(testStr, new String(readBytes));

		cache.release();
	}

	@Test
	public void testCreateOutputStream() throws Exception {
		File tmpFolder = tmp.newFolder();
		final CheckpointCache cache = new CheckpointCache(new JobID(), tmpFolder.getAbsolutePath(), 10000, 10000, mock(CheckpointCacheManager.class), Executors.directExecutor());

		StateHandleID handleID1 = new StateHandleID("handle1");
		CheckpointCache.CachedOutputStream outputStream = cache.createOutputStream(1, handleID1);

		final String testStr = "test str";
		outputStream.write(testStr.getBytes(), 0, testStr.length());
		outputStream.closeAndGetHandle();

		cache.commitCache(1);

		FSDataInputStream inputStream = cache.openInputStream(handleID1);
		byte[] readBytes = new byte[testStr.length()];
		inputStream.read(readBytes);
		Assert.assertEquals(testStr, new String(readBytes));

		cache.release();
	}

	@Test
	public void testDiscardOutputStream() throws Exception {
		File tmpFolder = tmp.newFolder();
		final CheckpointCache cache = new CheckpointCache(new JobID(), tmpFolder.getAbsolutePath(), 10000, 10000, mock(CheckpointCacheManager.class), Executors.directExecutor());

		StateHandleID handleID1 = new StateHandleID("handle1");
		CheckpointCache.CachedOutputStream outputStream = cache.createOutputStream(1, handleID1);

		final String testStr = "test str";
		outputStream.write(testStr.getBytes(), 0, testStr.length());
		outputStream.discard();
		outputStream.closeAndGetHandle();
		Assert.assertEquals(cache.getPendingCheckpointCacheSize(), 0);

		cache.release();
	}

	@Test
	public void testOnlyMaintainTheLastCheckpointCache() throws Exception {
		File tmpFolder = tmp.newFolder();
		final CheckpointCache cache = new CheckpointCache(new JobID(), tmpFolder.getAbsolutePath(), 10000, 10000, mock(CheckpointCacheManager.class), Executors.directExecutor());

		StateHandleID handleID1 = new StateHandleID("handle1");
		CheckpointCache.CachedOutputStream outputStream = cache.createOutputStream(1, handleID1);
		final String testStr = "test str";
		outputStream.write(testStr.getBytes(), 0, testStr.length());
		outputStream.closeAndGetHandle();
		outputStream.close();
		cache.commitCache(1);

		Assert.assertEquals(cache.getCompletedCheckpointCacheSize(), 1);

		StateHandleID handleID2 = new StateHandleID("handle2");
		CheckpointCache.CachedOutputStream outputStream2 = cache.createOutputStream(2, handleID2);
		final String testStr2 = "test str2";
		outputStream2.write(testStr2.getBytes(), 0, testStr2.length());
		outputStream2.closeAndGetHandle();
		outputStream2.close();
		cache.commitCache(2);

		Assert.assertEquals(cache.getCompletedCheckpointCacheSize(), 1);

		cache.release();
	}

	@Test
	public void testClose() throws Exception {
		File tmpFolder = tmp.newFolder();
		final CheckpointCache cache = new CheckpointCache(new JobID(), tmpFolder.getAbsolutePath(), 10000, 10000, mock(CheckpointCacheManager.class), Executors.directExecutor());

		for (int i = 0; i < 5; ++i) {
			for (int j = 0; j < 10; ++j) {
				final StateHandleID handleID = new StateHandleID("handle_" + i + "_" + j);
				final CheckpointCache.CachedOutputStream output = cache.createOutputStream(i, handleID);
				String testStr = "123";
				output.write(testStr.getBytes(), 0, testStr.getBytes().length);
				output.closeAndGetHandle();
				output.close();
			}
		}

		Assert.assertEquals(cache.getCompletedCheckpointCacheSize(), 0);;
		Assert.assertEquals(cache.getPendingCheckpointCacheSize(), 5);

		for (int i = 0; i < 3; ++i)  {
			cache.commitCache(i);
		}

		Assert.assertEquals(1, cache.getCompletedCheckpointCacheSize() );
		Assert.assertEquals(2, cache.getPendingCheckpointCacheSize());

		cache.release();

		Assert.assertEquals(0, cache.getCompletedCheckpointCacheSize());
		Assert.assertEquals( 0, cache.getPendingCheckpointCacheSize());
	}

	@Test
	public void testReCache() throws Exception {
		File tmpFolder = tmp.newFolder();
		final CheckpointCache cache = new CheckpointCache(new JobID(), tmpFolder.getAbsolutePath(), 10000, 10000, mock(CheckpointCacheManager.class), Executors.directExecutor());

		CachedStreamStateHandle[] cachedHandles = new CachedStreamStateHandle[5];
		final String testStr = "test re-cache logic.";
		StreamStateHandle[] cacheFilePaths = new StreamStateHandle[5];

		// checkpoint
		for (int i = 0; i < 5; ++i) {
			// cache stream
			final StateHandleID handleID = new StateHandleID("cache_" + i);
			final CheckpointCache.CachedOutputStream output = cache.createOutputStream(1, handleID);
			output.write(testStr.getBytes(), 0, testStr.getBytes().length);
			cacheFilePaths[i] = output.closeAndGetHandle();
			output.close();

			// remote output stream
			final String remoteFilePath = tmp.newFile("remote_" + i).getAbsolutePath();
			OutputStream outputStream = new FileOutputStream(remoteFilePath);
			outputStream.write(testStr.getBytes(), 0, testStr.getBytes().length);
			cachedHandles[i] = new CachedStreamStateHandle(handleID, new FileStateHandle(new Path(remoteFilePath), testStr.getBytes().length));
		}
		cache.commitCache(1);

		// delete cache file
		for (int i = 0; i < 5; ++i) {
			cacheFilePaths[i].discardState();
		}

		// read from cached handle, this should read from remote
		for (int i = 0; i < 5; ++i) {
			CachedStreamStateHandle cachedStreamStateHandle = cachedHandles[i];
			cachedStreamStateHandle.setCheckpointCache(cache);
			cachedStreamStateHandle.reCache(true);
			FSDataInputStream inputStream = cachedStreamStateHandle.openInputStream();
			byte[] bytes = new byte[1024];
			int n = inputStream.read(bytes);
			inputStream.close();
			Assert.assertEquals(n, testStr.length());
		}
		cache.commitCache(CheckpointCache.CHECKPOINT_ID_FOR_RESTORE);

		// read from cached handle again, this should read from cache
		for (int i = 0; i < 5; ++i) {
			CachedStreamStateHandle cachedStreamStateHandle = cachedHandles[i];
			cachedStreamStateHandle.setCheckpointCache(cache);
			cachedStreamStateHandle.reCache(true);
			FSDataInputStream inputStream = cachedStreamStateHandle.openInputStream();
			byte[] bytes = new byte[1024];
			int n  = inputStream.read(bytes);
			Assert.assertEquals(n, testStr.length());
		}

		// check refer count
		SharedCacheRegistry register = cache.getSharedCacheRegister();
		for (Map.Entry<CheckpointCache.CacheKey, CheckpointCache.CacheEntry> entry : register.getIterable()) {
			Assert.assertEquals(1, entry.getValue().getReferenceCount());
		}
	}

	@Test
	public void testCacheRegister() throws Exception {

		File tmpFolder = tmp.newFolder();
		final CheckpointCache cache = new CheckpointCache(new JobID(), tmpFolder.getAbsolutePath(), 10000, 10000, mock(CheckpointCacheManager.class), Executors.directExecutor());
		StringBuilder builder = new StringBuilder(4097);
		for (int i = 0; i < 500; ++i) {
			builder.append("1234567890");
		}
		final String testStr = builder.toString();
		StreamStateHandle[] cacheHandles = new StreamStateHandle[5];

		// checkpoint
		for (int i = 0; i < 5; ++i) {
			// cache stream
			final StateHandleID handleID = new StateHandleID("cache_" + i);
			final CheckpointCache.CachedOutputStream output = cache.createOutputStream(1, handleID);
			output.write(testStr.getBytes(), 0, testStr.getBytes().length);
			cacheHandles[i] = output.closeAndGetHandle();
			output.close();
		}
		cache.commitCache(1);

		// checkpoint
		for (int i = 4; i < 6; ++i) {
			// cache stream
			final StateHandleID handleID = new StateHandleID("cache_" + i);
			final CheckpointCache.CachedOutputStream output = cache.createOutputStream(2, handleID);
			output.write(testStr.getBytes(), 0, testStr.getBytes().length);
			output.closeAndGetHandle();
			output.close();
		}
		cache.commitCache(2);

		for (int i = 0; i < 4; ++i) {
			try {
				cacheHandles[i].openInputStream();
				Assert.fail();
			} catch (IOException expected) {}
		}
	}
}
