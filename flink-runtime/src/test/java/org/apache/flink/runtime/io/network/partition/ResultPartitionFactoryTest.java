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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.shuffle.PartitionDescriptorBuilder;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link ResultPartitionFactory}.
 */
@SuppressWarnings("StaticVariableUsedBeforeInitialization")
public class ResultPartitionFactoryTest extends TestLogger {

	private static final String tempDir = EnvironmentInformation.getTemporaryFileDirectory();
	private static final int SEGMENT_SIZE = 64;

	private static FileChannelManager fileChannelManager;

	@BeforeClass
	public static void setUp() {
		fileChannelManager = new FileChannelManagerImpl(new String[] {tempDir}, "testing");
	}

	@AfterClass
	public static void shutdown() throws Exception {
		fileChannelManager.close();
	}

	@Test
	public void testBoundedBlockingSubpartitionsCreated() {
		final BoundedBlockingResultPartition resultPartition = (BoundedBlockingResultPartition) createResultPartition(ResultPartitionType.BLOCKING);
		Arrays.stream(resultPartition.subpartitions).forEach(sp -> assertThat(sp, instanceOf(BoundedBlockingSubpartition.class)));
	}

	@Test
	public void testPipelinedSubpartitionsCreated() {
		final PipelinedResultPartition resultPartition = (PipelinedResultPartition) createResultPartition(ResultPartitionType.PIPELINED);
		Arrays.stream(resultPartition.subpartitions).forEach(sp -> assertThat(sp, instanceOf(PipelinedSubpartition.class)));
	}

	@Test
	public void testConsumptionOnReleaseForPipelined() {
		final ResultPartition resultPartition = createResultPartition(ResultPartitionType.PIPELINED);

		resultPartition.onConsumedSubpartition(0);

		assertTrue(resultPartition.isReleased());
	}

	@Test
	public void testNoConsumptionOnReleaseForBlocking() {
		final ResultPartition resultPartition = createResultPartition(ResultPartitionType.BLOCKING);

		resultPartition.onConsumedSubpartition(0);

		assertFalse(resultPartition.isReleased());
	}

	private static ResultPartition createResultPartition(ResultPartitionType partitionType) {
		final ResultPartitionManager manager = new ResultPartitionManager();

		final ResultPartitionFactory factory = new ResultPartitionFactory(
			manager,
			fileChannelManager,
			new NetworkBufferPool(1, SEGMENT_SIZE),
			BoundedBlockingSubpartitionType.AUTO,
			1,
			1,
			SEGMENT_SIZE,
			false,
			"LZ4",
			Integer.MAX_VALUE);

		final ResultPartitionDeploymentDescriptor descriptor = new ResultPartitionDeploymentDescriptor(
			PartitionDescriptorBuilder
				.newBuilder()
				.setPartitionType(partitionType)
				.build(),
			NettyShuffleDescriptorBuilder.newBuilder().buildLocal(),
			1,
			true
		);

		// guard our test assumptions
		assertEquals(1, descriptor.getNumberOfSubpartitions());

		final ResultPartition partition =  factory.create("test", 0, descriptor);
		manager.registerResultPartition(partition);

		return partition;
	}
}
