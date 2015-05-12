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


import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertFalse;

public class ResultPartitionManagerTest {

	@Test
	public void testFreeCachedResultPartitions() {
		ResultPartitionManager resultPartitionManager = new ResultPartitionManager();

		ResultPartition resultPartition1 = Mockito.mock(ResultPartition.class);
		ResultPartition resultPartition2 = Mockito.mock(ResultPartition.class);
		ResultPartition resultPartition3 = Mockito.mock(ResultPartition.class);

		ResultPartitionID partitionID1 = new ResultPartitionID();
		ResultPartitionID partitionID2 = new ResultPartitionID();
		ResultPartitionID partitionID3 = new ResultPartitionID();

		Mockito.when(resultPartition1.getPartitionId()).thenReturn(partitionID1);
		Mockito.when(resultPartition1.isFinished()).thenReturn(true);
		Mockito.when(resultPartition1.getPartitionType()).thenReturn(ResultPartitionType.BLOCKING);
		Mockito.when(resultPartition1.getTotalNumberOfBuffers()).thenReturn(5);


		Mockito.when(resultPartition2.getPartitionId()).thenReturn(partitionID2);
		Mockito.when(resultPartition2.isFinished()).thenReturn(true);
		Mockito.when(resultPartition2.getPartitionType()).thenReturn(ResultPartitionType.BLOCKING);
		Mockito.when(resultPartition2.getTotalNumberOfBuffers()).thenReturn(3);


		Mockito.when(resultPartition3.getPartitionId()).thenReturn(partitionID3);
		Mockito.when(resultPartition3.isFinished()).thenReturn(true);
		Mockito.when(resultPartition3.getPartitionType()).thenReturn(ResultPartitionType.BLOCKING);
		Mockito.when(resultPartition3.getTotalNumberOfBuffers()).thenReturn(2);

		try {
			resultPartitionManager.registerResultPartition(resultPartition1);
			resultPartitionManager.registerResultPartition(resultPartition2);
			resultPartitionManager.registerResultPartition(resultPartition3);
		} catch (IOException e) {
			e.printStackTrace();
			fail("failed to register ResultPartition");
		}

		resultPartitionManager.releasePartitionsProducedBy(partitionID1.getProducerId());
		resultPartitionManager.releasePartitionsProducedBy(partitionID2.getProducerId());
		resultPartitionManager.releasePartitionsProducedBy(partitionID3.getProducerId());

		assertFalse(resultPartitionManager.releaseLeastRecentlyUsedCachedPartitions(10000));
		assertFalse(resultPartitionManager.releaseLeastRecentlyUsedCachedPartitions(11));
		assertTrue(resultPartitionManager.releaseLeastRecentlyUsedCachedPartitions(10));
		assertFalse(resultPartitionManager.releaseLeastRecentlyUsedCachedPartitions(10));
	}
}
