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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.util.EnvironmentInformation;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.buildSomeBuffer;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledFinishedBufferConsumer;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests that read the BoundedBlockingSubpartition with multiple threads in parallel.
 */
public class FileChannelBoundedDataTest extends BoundedDataTestBase {

	private static final String tempDir = EnvironmentInformation.getTemporaryFileDirectory();

	private static FileChannelManager fileChannelManager;

	@BeforeClass
	public static void setUp() {
		fileChannelManager = new FileChannelManagerImpl(new String[] {tempDir}, "testing");
	}

	@AfterClass
	public static void shutdown() throws Exception {
		fileChannelManager.close();
	}

	@Override
	protected boolean isRegionBased() {
		return false;
	}

	@Override
	protected BoundedData createBoundedData(Path tempFilePath) throws IOException {
		return FileChannelBoundedData.create(tempFilePath, BUFFER_SIZE);
	}

	@Override
	protected BoundedData createBoundedDataWithRegion(Path tempFilePath, int regionSize) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Test
	public void testReadNextBuffer() throws Exception {
		final int numberOfBuffers = 3;
		try (final BoundedData data = createBoundedData()) {
			writeBuffers(data, numberOfBuffers);

			final BoundedData.Reader reader = data.createReader();
			final Buffer buffer1 = reader.nextBuffer();
			final Buffer buffer2 = reader.nextBuffer();

			assertNotNull(buffer1);
			assertNotNull(buffer2);
			// there are only two available memory segments for reading data
			assertNull(reader.nextBuffer());

			// cleanup
			buffer1.recycleBuffer();
			buffer2.recycleBuffer();
		}
	}

	@Test
	public void testRecycleBufferForNotifyingSubpartitionView() throws Exception {
		final int numberOfBuffers = 2;
		try (final BoundedData data = createBoundedData()) {
			writeBuffers(data, numberOfBuffers);

			final VerifyNotificationResultSubpartitionView subpartitionView = new VerifyNotificationResultSubpartitionView();
			final BoundedData.Reader reader = data.createReader(subpartitionView);
			final Buffer buffer1 = reader.nextBuffer();
			final Buffer buffer2 = reader.nextBuffer();
			assertNotNull(buffer1);
			assertNotNull(buffer2);

			assertFalse(subpartitionView.isAvailable);
			buffer1.recycleBuffer();
			// the view is notified while recycling buffer if reader has not tagged finished
			assertTrue(subpartitionView.isAvailable);

			subpartitionView.resetAvailable();
			assertFalse(subpartitionView.isAvailable);

			// the next buffer is null to make reader tag finished
			assertNull(reader.nextBuffer());

			buffer2.recycleBuffer();
			// the view is not notified while recycling buffer if reader already finished
			assertFalse(subpartitionView.isAvailable);
		}
	}

	@Test
	public void testRecycleBufferForNotifyingBufferAvailabilityListener() throws Exception {
		final ResultSubpartition subpartition = createFileBoundedBlockingSubpartition();
		final int numberOfBuffers = 2;
		writeBuffers(subpartition, numberOfBuffers);

		final VerifyNotificationBufferAvailabilityListener listener = new VerifyNotificationBufferAvailabilityListener();
		final ResultSubpartitionView subpartitionView = subpartition.createReadView(listener);
		// the notification is triggered while creating view
		assertTrue(listener.isAvailable);

		listener.resetAvailable();
		assertFalse(listener.isAvailable);

		final BufferAndBacklog buffer1 = subpartitionView.getNextBuffer();
		final BufferAndBacklog buffer2 = subpartitionView.getNextBuffer();
		assertNotNull(buffer1);
		assertNotNull(buffer2);

		// the next buffer is null in view because FileBufferReader has no available buffers for reading ahead
		assertFalse(subpartitionView.isAvailable(Integer.MAX_VALUE));
		// recycle a buffer to trigger notification of data available
		buffer1.buffer().recycleBuffer();
		assertTrue(listener.isAvailable);

		// cleanup
		buffer2.buffer().recycleBuffer();
		subpartitionView.releaseAllResources();
		subpartition.release();
	}

	private static ResultSubpartition createFileBoundedBlockingSubpartition() {
		final ResultPartition resultPartition = new ResultPartitionBuilder()
			.setNetworkBufferSize(BUFFER_SIZE)
			.setResultPartitionType(ResultPartitionType.BLOCKING)
			.setBoundedBlockingSubpartitionType(BoundedBlockingSubpartitionType.FILE)
			.setFileChannelManager(fileChannelManager)
			.build();
		return resultPartition.subpartitions[0];
	}

	private static void writeBuffers(BoundedData data, int numberOfBuffers) throws IOException {
		for (int i = 0; i < numberOfBuffers; i++) {
			data.writeBuffer(buildSomeBuffer(BUFFER_SIZE));
		}
		data.finishWrite();
	}

	private static void writeBuffers(ResultSubpartition subpartition, int numberOfBuffers) throws IOException {
		for (int i = 0; i < numberOfBuffers; i++) {
			subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
		}
		subpartition.finish();
	}

	/**
	 * This subpartition view is used for verifying the {@link ResultSubpartitionView#notifyDataAvailable()}
	 * was ever called before.
	 */
	private static class VerifyNotificationResultSubpartitionView extends NoOpResultSubpartitionView {

		private boolean isAvailable;

		@Override
		public void notifyDataAvailable() {
			isAvailable = true;
		}

		private void resetAvailable() {
			isAvailable = false;
		}
	}

	/**
	 * This listener is used for verifying the notification logic in {@link ResultSubpartitionView#notifyDataAvailable()}.
	 */
	private static class VerifyNotificationBufferAvailabilityListener implements BufferAvailabilityListener {

		private boolean isAvailable;

		@Override
		public void notifyDataAvailable() {
			isAvailable = true;
		}

		private void resetAvailable() {
			isAvailable = false;
		}
	}
}
