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
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.NoOpIOManager;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for the {@link ResultPartitionFactory}.
 */
public class ResultPartitionFactoryTest extends TestLogger {

	@Test
	public void testForceConsumptionOnReleaseEnabled() {
		testForceConsumptionOnRelease(true);
	}

	@Test
	public void testForceConsumptionOnReleaseDisabled() {
		testForceConsumptionOnRelease(false);
	}

	private static void testForceConsumptionOnRelease(boolean forceConsumptionOnRelease) {
		ResultPartitionFactory factory = new ResultPartitionFactory(
			new ResultPartitionManager(),
			new NoOpIOManager(),
			new NetworkBufferPool(1, 64, 1),
			1,
			1,
			forceConsumptionOnRelease
		);

		final ResultPartitionDeploymentDescriptor descriptor = new ResultPartitionDeploymentDescriptor(
			new PartitionDescriptor(
				new IntermediateDataSetID(),
				new IntermediateResultPartitionID(),
				ResultPartitionType.BLOCKING,
				1,
				0),
			ResultPartitionID::new,
			1,
			true
		);

		final ResultPartition test = factory.create("test", new ExecutionAttemptID(), descriptor);

		if (forceConsumptionOnRelease) {
			assertThat(test, instanceOf(ReleaseOnConsumptionResultPartition.class));
		} else {
			assertThat(test, not(instanceOf(ReleaseOnConsumptionResultPartition.class)));
		}
	}
}
