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

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.partition.PartitionTestUtils.createView;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests for the availability handling of the BoundedBlockingSubpartitions with not constant
 * availability.
 */
public class BoundedBlockingSubpartitionAvailabilityTest {

	@ClassRule
	public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

	private static final int BUFFER_SIZE = 32 * 1024;

	@Test
	public void testInitiallyNotAvailable() throws Exception {
		final ResultSubpartition subpartition = createPartitionWithData(10);
		final CountingAvailabilityListener listener = new CountingAvailabilityListener();

		// test
		final ResultSubpartitionView subpartitionView = createView(subpartition, listener);

		// assert
		assertEquals(0, listener.numNotifications);

		// cleanup
		subpartitionView.releaseAllResources();
		subpartition.release();
	}

	@Test
	public void testNotAvailableWhenEmpty() throws Exception {
		// setup
		final ResultSubpartition subpartition = createPartitionWithData(100_000);
		final ResultSubpartitionView reader = subpartition.createReadView(new NoOpBufferAvailablityListener());

		// test
		drainAllData(reader);

		// assert
		assertFalse(reader.isAvailable(Integer.MAX_VALUE));

		// cleanup
		reader.releaseAllResources();
		subpartition.release();
	}

	// ------------------------------------------------------------------------

	private static ResultSubpartition createPartitionWithData(int numberOfBuffers) throws IOException {
		BoundedBlockingResultPartition parent = (BoundedBlockingResultPartition) new ResultPartitionBuilder()
			.setResultPartitionType(ResultPartitionType.BLOCKING_PERSISTENT)
			.setBoundedBlockingSubpartitionType(BoundedBlockingSubpartitionType.FILE)
			.setFileChannelManager(new FileChannelManagerImpl(new String[] { TMP_FOLDER.newFolder().toString() }, "data"))
			.setNetworkBufferSize(BUFFER_SIZE)
			.build();

		ResultSubpartition partition = parent.getAllPartitions()[0];

		writeBuffers(partition, numberOfBuffers);
		partition.finish();

		return partition;
	}

	private static void writeBuffers(ResultSubpartition partition, int numberOfBuffers) throws IOException {
		for (int i = 0; i < numberOfBuffers; i++) {
			partition.add(BufferBuilderTestUtils.createFilledFinishedBufferConsumer(BUFFER_SIZE));
		}
	}

	private static void drainAllData(ResultSubpartitionView reader) throws Exception {
		PartitionData data;
		while ((data = reader.getNextData()) != null) {
			Buffer buffer = data.getBuffer(MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE));
			buffer.recycleBuffer();
		}
	}
}
