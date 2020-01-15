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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link NettyShuffleMaster}.
 */
public class NettyShuffleMasterTest extends TestLogger {

	@Test
	public void testGetShuffleMemoryForTask() {
		final int numBuffersPerChannel = 10;
		final int bufferBytes = 4096;

		final Configuration configuration = new Configuration();
		configuration.setInteger(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL, numBuffersPerChannel);
		configuration.setString(TaskManagerOptions.MEMORY_SEGMENT_SIZE, String.valueOf(bufferBytes));

		final NettyShuffleMaster shuffleMaster = new NettyShuffleMaster(configuration);

		final int numChannelsOfGate1 = 1;
		final int numChannelsOfGate2 = 2;
		final Map<IntermediateDataSetID, Integer> numbersOfInputGateChannels = new HashMap<IntermediateDataSetID, Integer>() {{
			put(new IntermediateDataSetID(), numChannelsOfGate1);
			put(new IntermediateDataSetID(), numChannelsOfGate2);
		}};
		final int numSubpartitionsOfResult1 = 3;
		final int numSubpartitionsOfResult2 = 4;
		final Map<IntermediateDataSetID, Integer> numbersOfResultSubpartitions = new HashMap<IntermediateDataSetID, Integer>() {{
			put(new IntermediateDataSetID(), numSubpartitionsOfResult1);
			put(new IntermediateDataSetID(), numSubpartitionsOfResult2);
		}};

		final TaskInputsOutputsDescriptor inputsOutputsDescriptor = TaskInputsOutputsDescriptor.from(
			numbersOfInputGateChannels,
			numbersOfResultSubpartitions);

		final int expectedBuffers = NettyShuffleUtils.computeRequiredNetworkBuffers(
			numBuffersPerChannel,
			numbersOfInputGateChannels.values(),
			numbersOfResultSubpartitions.values());
		assertEquals(
			new MemorySize(4096 * expectedBuffers),
			shuffleMaster.getShuffleMemoryForTask(inputsOutputsDescriptor));
	}
}
