/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.	See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.	The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.	You may obtain a copy of the License at
 *
 *		 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.external.writer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.partition.external.PartitionIndex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.fail;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * Test base for persistent file writer.
 */
public abstract class PersistentFileWriterTestBase {
	public static final int PAGE_SIZE = 4096;
	public static final int NUM_PAGES = 100;
	public static final int MEMORY_SIZE = PAGE_SIZE * NUM_PAGES;
	public static final AbstractInvokable parentTask = mock(AbstractInvokable.class);

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	protected IOManager ioManager;
	protected MemoryManager memoryManager;
	protected TypeSerializer<Integer> serializer;

	@Before
	public void before() {
		this.ioManager = new IOManagerAsync();
		this.memoryManager = new MemoryManager(MEMORY_SIZE, 1, 4096, MemoryType.HEAP, true);
		this.serializer = IntSerializer.INSTANCE;
	}

	@After
	public void after() {
		this.ioManager.shutdown();
		if (!this.ioManager.isProperlyShutDown()) {
			fail("I/O Manager was not properly shut down.");
		}

		if (this.memoryManager != null) {
			this.memoryManager.releaseAll(parentTask);
			Assert.assertTrue("Memory leak: not all segments have been returned to the memory manager.",
				this.memoryManager.verifyEmpty());

			this.memoryManager.shutdown();
			this.memoryManager = null;
		}
	}

	@Test
	public void testNormal() throws Exception {
		test(4, 100, false, new ChannelSelector<Integer>() {
			private int nextIndex = 0;

			@Override
			public int selectChannel(Integer record, int numChannels) {
				return (nextIndex++) % numChannels;
			}
		});
	}

	@Test
	public void testMoreRecords() throws Exception {
		test(4, 100000, false, new ChannelSelector<Integer>() {
			private int nextIndex = 0;

			@Override
			public int selectChannel(Integer record, int numChannels) {
				return (nextIndex++) % numChannels;
			}
		});
	}

	@Test
	public void testMorePartitions() throws Exception {
		test(40, 100000, false, new ChannelSelector<Integer>() {
			private int nextIndex = 0;

			@Override
			public int selectChannel(Integer record, int numChannels) {
				return (nextIndex++) % numChannels;
			}
		});
	}

	@Test
	public void testBroadcast() throws Exception {
		test(40, 100000, true, null);
	}

	protected void test(int numberPartitions, int numberOfRecords, boolean isBroadcast, ChannelSelector<Integer> channelSelector)
		throws Exception {
		String partitionRootPath = temporaryFolder.newFolder().getAbsolutePath() + "/";

		PersistentFileWriter<Integer> shuffleWriter = createFileWriter(numberPartitions, partitionRootPath);

		List<Set<Integer>> expectedResult = new ArrayList<>();
		for (int i = 0; i < numberPartitions; ++i) {
			expectedResult.add(new HashSet<>());
		}

		int[] allChannels = new int[numberPartitions];
		for(int i = 0; i < numberPartitions; ++i) {
			allChannels[i] = i;
		}

		for (int i = 0; i < numberOfRecords; ++i) {
			int[] channels;

			if (isBroadcast) {
				channels = allChannels;
			} else {
				channels = new int[]{channelSelector.selectChannel(i, numberPartitions)};
			}

			shuffleWriter.add(i, channels);

			for (int channel : channels) {
				expectedResult.get(channel).add(i);
			}
		}

		shuffleWriter.finish();
		List<List<PartitionIndex>> partitionIndices = shuffleWriter.generatePartitionIndices();

		List<Set<Integer>> actualResult = new ArrayList<>();
		for (int i = 0; i < numberPartitions; ++i) {
			actualResult.add(new HashSet<>());
		}

		for (int i = 0; i < numberPartitions; ++i) {
			MutableObjectIterator<Integer> iterator = createResultIterator(numberPartitions, partitionRootPath, partitionIndices, i);

			Integer result;
			while ((result = iterator.next()) != null) {
				actualResult.get(i).add(result);
			}
		}

		for (int i = 0; i < numberPartitions; ++i) {
			assertEquals("Partition " + i + "'s result check fail", expectedResult.get(i), actualResult.get(i));
		}

		shuffleWriter.clear();
	}

	protected abstract PersistentFileWriter<Integer> createFileWriter(int numberPartitions, String partitionRootPath) throws Exception;

	protected abstract MutableObjectIterator<Integer> createResultIterator(int numPartitions,
																			String partitionRootPath,
																			List<List<PartitionIndex>> partitionIndices,
																			int subpartitionIndex) throws Exception;
}
