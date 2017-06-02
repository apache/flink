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

package org.apache.flink.runtime.deployment;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link ResultPartitionDeploymentDescriptor}.
 */
public class ResultPartitionDeploymentDescriptorTest {

	/**
	 * Tests simple de/serialization.
	 */
	@Test
	public void testSerialization() throws Exception {
		// Expected values
		IntermediateDataSetID resultId = new IntermediateDataSetID();
		IntermediateResultPartitionID partitionId = new IntermediateResultPartitionID();
		ResultPartitionType partitionType = ResultPartitionType.PIPELINED;
		int numberOfSubpartitions = 24;

		ResultPartitionDeploymentDescriptor orig =
				new ResultPartitionDeploymentDescriptor(
						resultId,
						partitionId,
						partitionType,
						numberOfSubpartitions,
						numberOfSubpartitions,
						true);

		ResultPartitionDeploymentDescriptor copy =
				CommonTestUtils.createCopySerializable(orig);

		assertEquals(resultId, copy.getResultId());
		assertEquals(partitionId, copy.getPartitionId());
		assertEquals(partitionType, copy.getPartitionType());
		assertEquals(numberOfSubpartitions, copy.getNumberOfSubpartitions());
		assertTrue(copy.sendScheduleOrUpdateConsumersMessage());
	}
}
