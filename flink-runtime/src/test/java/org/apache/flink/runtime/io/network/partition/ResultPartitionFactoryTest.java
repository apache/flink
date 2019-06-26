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
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for the {@link ResultPartitionFactory}.
 */
public class ResultPartitionFactoryTest extends TestLogger {

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

	@Test
	public void testConsumptionOnReleaseEnabled() {
		final ResultPartition resultPartition = createResultPartition(ShuffleDescriptor.ReleaseType.AUTO);
		assertThat(resultPartition, instanceOf(ReleaseOnConsumptionResultPartition.class));
	}

	@Test
	public void testConsumptionOnReleaseDisabled() {
		final ResultPartition resultPartition = createResultPartition(ShuffleDescriptor.ReleaseType.MANUAL);
		assertThat(resultPartition, not(instanceOf(ReleaseOnConsumptionResultPartition.class)));
	}

	private static ResultPartition createResultPartition(ShuffleDescriptor.ReleaseType releaseType) {
		ResultPartitionFactory factory = new ResultPartitionFactory(
			new ResultPartitionManager(),
			fileChannelManager,
			new NetworkBufferPool(1, 64, 1),
			1,
			1
		);

		ResultPartitionType partitionType = ResultPartitionType.BLOCKING;
		final ResultPartitionDeploymentDescriptor descriptor = new ResultPartitionDeploymentDescriptor(
			new PartitionDescriptor(
				new IntermediateDataSetID(),
				new IntermediateResultPartitionID(),
				partitionType,
				1,
				0),
			NettyShuffleDescriptorBuilder.newBuilder().setBlocking(partitionType.isBlocking()).buildLocal(),
			1,
			true,
			releaseType
		);

		return factory.create("test", descriptor);
	}
}
