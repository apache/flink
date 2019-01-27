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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.FixedLengthBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Test the view of external result partition.
 */
@RunWith(Parameterized.class)
public class ExternalBlockSubpartitionViewTest {
	private static final int SEGMENT_SIZE = 128;

	private static final int NUM_BUFFERS = 20;

	private static final int[] TOTAL_BYTES_EACH_SUBPARTITION = new int[]{1300, 2600, 3900, 5200};

	private static final int MERGED_FILE_TOTAL_FILES = 5;

	private IOManager ioManager;

	private FixedLengthBufferPool bufferPool;

	private int subpartitionIndex;

	private int bytesRead;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	private final PersistentFileType fileType;

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
			/** Normal cases */
			{PersistentFileType.HASH_PARTITION_FILE},
			{PersistentFileType.MERGED_PARTITION_FILE},
		});
	}

	public ExternalBlockSubpartitionViewTest(PersistentFileType fileType) throws Exception {
		this.fileType = fileType;
	}

	@Before
	public void before() {
		this.ioManager = new IOManagerAsync();
		this.bufferPool = new FixedLengthBufferPool(NUM_BUFFERS, SEGMENT_SIZE, MemoryType.HEAP);
	}

	@After
	public void after() {
		ioManager.shutdown();

		assertEquals("Not all buffers returned", NUM_BUFFERS, bufferPool.getNumberOfAvailableMemorySegments());
		this.bufferPool.lazyDestroy();
	}

	@Test
	public void testInitializedOnFirstRead() throws Exception {
		final int subpartitionIndex = 2;
		setupCheckForSubpartition(subpartitionIndex);

		ExternalBlockResultPartitionMeta meta = spy(createFilesAndMeta());
		ExecutorService executor = null;

		try {
			executor = Executors.newFixedThreadPool(1);

			ViewReader viewReader = new ViewReader();
			ExternalBlockSubpartitionView view = new ExternalBlockSubpartitionView(meta,
				subpartitionIndex,
				executor,
				meta.getResultPartitionID(),
				bufferPool,
				0,
				viewReader);
			viewReader.setView(view);

			view.notifyCreditAdded(2);
			checkBufferAndRecycle(viewReader.getNextBufferBlocking());
			checkBufferAndRecycle(viewReader.getNextBufferBlocking());

			verify(meta).initialize();
			assertEquals(TOTAL_BYTES_EACH_SUBPARTITION[subpartitionIndex], view.getTotalLength());
			assertNotNull(view.getMetaIterator());
		} finally {
			if (executor != null) {
				executor.shutdownNow();
			}
		}
	}

	@Test(timeout = 60000)
	public void testManagingCredit() throws Exception {
		final int subpartitionIndex = 2;
		setupCheckForSubpartition(subpartitionIndex);

		ExternalBlockResultPartitionMeta meta = spy(createFilesAndMeta());

		ExecutorService executor = null;

		try {
			executor = spy(Executors.newFixedThreadPool(1));

			ViewReader viewReader = new ViewReader();
			ExternalBlockSubpartitionView view = new ExternalBlockSubpartitionView(meta,
				subpartitionIndex,
				executor,
				meta.getResultPartitionID(),
				bufferPool,
				0,
				viewReader);
			viewReader.setView(view);

			view.notifyCreditAdded(2);

			// Check the executor is submitting on the first batch of credits.
			verify(executor).submit(eq(view));

			checkBufferAndRecycle(viewReader.getNextBufferBlocking());
			checkBufferAndRecycle(viewReader.getNextBufferBlocking());

			while (view.isRunning()) {
				Thread.sleep(500);
			}

			assertEquals(0, view.getCurrentCredit());
			view.notifyCreditAdded(2);
			verify(executor, times(2)).submit(eq(view));

			checkBufferAndRecycle(viewReader.getNextBufferBlocking());
			checkBufferAndRecycle(viewReader.getNextBufferBlocking());
		} finally {
			if (executor != null) {
				executor.shutdownNow();
			}
		}
	}

	@Test(timeout = 60000)
	public void testGetNextBuffer() throws Exception {
		final int subpartitionIndex = 2;
		setupCheckForSubpartition(subpartitionIndex);

		ExternalBlockResultPartitionMeta meta = spy(createFilesAndMeta());

		ExecutorService executor = null;

		try {
			executor = spy(Executors.newFixedThreadPool(1));

			ViewReader viewReader = new ViewReader();
			ExternalBlockSubpartitionView view = new ExternalBlockSubpartitionView(meta,
				subpartitionIndex,
				executor,
				meta.getResultPartitionID(),
				bufferPool,
				0,
				viewReader);
			viewReader.setView(view);

			Random random = new Random();
			int nextBufferIndex = 0;

			int remainingBytesToRead = TOTAL_BYTES_EACH_SUBPARTITION[subpartitionIndex];
			while (remainingBytesToRead > SEGMENT_SIZE) {
				int nextCredit = Math.max(random.nextInt((int) Math.ceil((double) remainingBytesToRead / SEGMENT_SIZE)) - 1, 1);
				view.notifyCreditAdded(nextCredit);

				for (int i = 0; i < nextCredit; ++i) {
					Buffer buffer = viewReader.getNextBufferBlocking();
					remainingBytesToRead -= buffer.getSize();
					checkBufferAndRecycle(buffer);

					if (i == nextCredit - 1) {
						assertFalse(view.isAvailable());
					}
				}
			}

			while (remainingBytesToRead > 0) {
				view.notifyCreditAdded(1);
				Buffer buffer = viewReader.getNextBufferBlocking();
				remainingBytesToRead -= buffer.getSize();
				checkBufferAndRecycle(buffer);
			}

			Buffer eof = viewReader.getNextBufferBlocking();
			assertFalse(eof.isBuffer());
			assertEquals(EndOfPartitionEvent.INSTANCE, EventSerializer.fromBuffer(eof, this.getClass().getClassLoader()));
			assertFalse(view.isAvailable());
		} finally {
			if (executor != null) {
				executor.shutdownNow();
			}
		}
	}

	@Test
	public void testReadFail() throws Exception {
		final int subpartitionIndex = 2;
		setupCheckForSubpartition(subpartitionIndex);

		ExternalBlockResultPartitionMeta meta = spy(createFilesAndMeta());

		ExecutorService executor = null;

		try {
			executor = spy(Executors.newFixedThreadPool(1));

			BufferAvailabilityListener availabilityListener = mock(BufferAvailabilityListener.class);
			ExternalBlockSubpartitionView view = spy(new ExternalBlockSubpartitionView(meta,
				subpartitionIndex,
				executor,
				meta.getResultPartitionID(),
				bufferPool,
				0,
				availabilityListener));

			// Remove the data files directly
			int numFilesToRemove = (fileType == PersistentFileType.HASH_PARTITION_FILE ? TOTAL_BYTES_EACH_SUBPARTITION.length : MERGED_FILE_TOTAL_FILES);
			for (int i = 0; i < numFilesToRemove; ++i) {
				boolean success =
					new File(ExternalBlockShuffleUtils.generateDataPath(meta.getResultPartitionDir(), i)).delete();
				assertTrue("Delete the data file failed", success);
			}

			view.notifyCreditAdded(1);

			// Should be notified in expected period.
			verify(availabilityListener, timeout(10000)).notifyDataAvailable();

			assertTrue(view.nextBufferIsEvent());
			assertNull(view.getNextBuffer());
			assertNotNull(view.getFailureCause());
		} finally {
			if (executor != null) {
				executor.shutdownNow();
			}
		}
	}

	// -------------------------------- Internal Utilities ------------------------------------

	private ExternalBlockResultPartitionMeta createFilesAndMeta() throws Exception {
		if (fileType == PersistentFileType.HASH_PARTITION_FILE) {
			return createHashFilesAndMeta(TOTAL_BYTES_EACH_SUBPARTITION);
		} else {
			int[] bytesPerFile = new int[TOTAL_BYTES_EACH_SUBPARTITION.length];
			for (int i = 0; i < TOTAL_BYTES_EACH_SUBPARTITION.length; ++i) {
				assert (TOTAL_BYTES_EACH_SUBPARTITION[i] % MERGED_FILE_TOTAL_FILES) == 0;
				bytesPerFile[i] = TOTAL_BYTES_EACH_SUBPARTITION[i] / MERGED_FILE_TOTAL_FILES;
			}

			return createMergeFilesAndMeta(bytesPerFile, MERGED_FILE_TOTAL_FILES);
		}
	}

	private ExternalBlockResultPartitionMeta createMergeFilesAndMeta(int[] bytesPerFileOfEachSubpartition, int numFiles) throws Exception {
		String root = tempFolder.newFolder().getAbsolutePath() + "/";
		FileSystem fs = FileSystem.getLocalFileSystem();

		List<List<PartitionIndex>> partitionIndicesList = new ArrayList<>();

		for (int fileIndex = 0; fileIndex < numFiles; ++fileIndex) {
			long bytesWritten = 0;
			List<PartitionIndex> partitionIndices = new ArrayList<>();

			BufferFileWriter writer = ioManager.createStreamFileWriter(
				new FileIOChannel.ID(ExternalBlockShuffleUtils.generateDataPath(root, fileIndex)));

			for (int i = 0; i < bytesPerFileOfEachSubpartition.length; ++i) {
				partitionIndices.add(new PartitionIndex(i, bytesWritten, bytesPerFileOfEachSubpartition[i]));

				long remainingBytesToWrite = bytesPerFileOfEachSubpartition[i];
				while (remainingBytesToWrite > 0) {
					long nextLengthToWrite = Math.min(remainingBytesToWrite, SEGMENT_SIZE);
					MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(SEGMENT_SIZE);
					NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
					for (int j = 0; j < nextLengthToWrite; j++) {
						buffer.asByteBuf().writeByte(fileIndex * bytesPerFileOfEachSubpartition[i] + i + j
							+ bytesPerFileOfEachSubpartition[i] - (int) remainingBytesToWrite);
					}

					bytesWritten = bytesWritten + nextLengthToWrite;
					remainingBytesToWrite -= nextLengthToWrite;

					writer.writeBlock(buffer);
				}
			}

			writer.close();
			partitionIndicesList.add(partitionIndices);
		}

		// write index and finish files
		for (int fileIndex = 0; fileIndex < numFiles; ++fileIndex) {
			String indexPath = ExternalBlockShuffleUtils.generateIndexPath(root, fileIndex);
			try (FSDataOutputStream indexOut = fs.create(new Path(indexPath), FileSystem.WriteMode.OVERWRITE)) {
				DataOutputView indexView = new DataOutputViewStreamWrapper(indexOut);
				ExternalBlockShuffleUtils.serializeIndices(partitionIndicesList.get(fileIndex), indexView);
			}
		}

		String finishedPath = ExternalBlockShuffleUtils.generateFinishedPath(root);
		try (FSDataOutputStream finishedOut = fs.create(new Path(finishedPath), FileSystem.WriteMode.OVERWRITE)) {
			DataOutputView finishedView = new DataOutputViewStreamWrapper(finishedOut);

			finishedView.writeInt(ExternalBlockResultPartitionMeta.SUPPORTED_PROTOCOL_VERSION);

			String externalFileType = PersistentFileType.MERGED_PARTITION_FILE.toString();
			finishedView.writeInt(externalFileType.length());
			finishedView.write(externalFileType.getBytes());
			finishedView.writeInt(numFiles);
			finishedView.writeInt(bytesPerFileOfEachSubpartition.length);
		}

		return new ExternalBlockResultPartitionMeta(new ResultPartitionID(), fs,
			new ExternalBlockResultPartitionManagerTest.MockResultPartitionFileInfo(root, root, 0, 0));
	}

	private ExternalBlockResultPartitionMeta createHashFilesAndMeta(int[] bytesEachSubpartition) throws Exception {
		String root = tempFolder.newFolder().getAbsolutePath() + "/";
		FileSystem fs = FileSystem.getLocalFileSystem();

		for (int i = 0; i < bytesEachSubpartition.length; ++i) {
			BufferFileWriter writer = ioManager.createStreamFileWriter(
				new FileIOChannel.ID(ExternalBlockShuffleUtils.generateDataPath(root, i)));
			long remainingBytesToWrite = bytesEachSubpartition[i];
			int bufferIndex = 0;
			while (remainingBytesToWrite > 0) {
				long nextLengthToWrite = Math.min(remainingBytesToWrite, SEGMENT_SIZE);
				MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(SEGMENT_SIZE);
				NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
				for (int j = 0; j < nextLengthToWrite; j++) {
					buffer.asByteBuf().writeByte(bufferIndex * SEGMENT_SIZE + i + j);
				}

				bufferIndex++;
				remainingBytesToWrite -= nextLengthToWrite;

				writer.writeBlock(buffer);
			}

			writer.close();
		}

		// write index and finish files
		List<PartitionIndex> partitionIndexList = new ArrayList<>();
		for (int i = 0; i < bytesEachSubpartition.length; ++i) {
			partitionIndexList.add(new PartitionIndex(i, 0, bytesEachSubpartition[i]));
		}
		String indexPath = ExternalBlockShuffleUtils.generateIndexPath(root, 0);
		try (FSDataOutputStream indexOut = fs.create(new Path(indexPath), FileSystem.WriteMode.OVERWRITE)) {
			DataOutputView indexView = new DataOutputViewStreamWrapper(indexOut);
			ExternalBlockShuffleUtils.serializeIndices(partitionIndexList, indexView);
		}

		String finishedPath = ExternalBlockShuffleUtils.generateFinishedPath(root);
		try (FSDataOutputStream finishedOut = fs.create(new Path(finishedPath), FileSystem.WriteMode.OVERWRITE)) {
			DataOutputView finishedView = new DataOutputViewStreamWrapper(finishedOut);

			finishedView.writeInt(ExternalBlockResultPartitionMeta.SUPPORTED_PROTOCOL_VERSION);

			String externalFileType = PersistentFileType.HASH_PARTITION_FILE.toString();
			finishedView.writeInt(externalFileType.length());
			finishedView.write(externalFileType.getBytes());
			finishedView.writeInt(1);
			finishedView.writeInt(bytesEachSubpartition.length);
		}

		return new ExternalBlockResultPartitionMeta(new ResultPartitionID(), fs,
			new ExternalBlockResultPartitionManagerTest.MockResultPartitionFileInfo(root, root, 0, 0));
	}

	private void setupCheckForSubpartition(int subpartitionIndex) {
		this.subpartitionIndex = subpartitionIndex;
		this.bytesRead = 0;
	}

	private void checkBufferAndRecycle(Buffer buffer) {
		assertTrue(buffer.isBuffer());
		assertTrue(buffer.getSize() > 0);
		ByteBuf byteBuf = buffer.asByteBuf();
		for (int i = 0; i < buffer.getSize(); i++) {
			byte actualValue = byteBuf.readByte();
			assertEquals("bytesRead: " + bytesRead + ", offset: " + i,
				(byte) ((bytesRead + i + subpartitionIndex) & 0x0ff), actualValue);
		}
		bytesRead += buffer.getSize();
		buffer.recycleBuffer();
	}

	private class ViewReader implements BufferAvailabilityListener {
		private final BlockingQueue<Buffer> bufferRead = new LinkedBlockingQueue<>();
		private ExternalBlockSubpartitionView view;

		public void setView(ExternalBlockSubpartitionView view) {
			this.view = view;
		}

		@Override
		public void notifyDataAvailable() {
			while (true) {
				ResultSubpartition.BufferAndBacklog bufferAndBacklog = view.getNextBuffer();
				bufferRead.add(bufferAndBacklog.buffer());

				if (!bufferAndBacklog.isMoreAvailable()) {
					break;
				}
			}
		}

		public Buffer getNextBufferBlocking() throws InterruptedException {
			return bufferRead.take();
		}
	}
}
