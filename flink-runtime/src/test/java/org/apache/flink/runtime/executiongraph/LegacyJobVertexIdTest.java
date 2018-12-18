/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.util.SerializedValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.mock;

public class LegacyJobVertexIdTest {

	@Test
	public void testIntroduceLegacyJobVertexIds() throws Exception {
		JobVertexID defaultId = new JobVertexID();
		JobVertexID legacyId1 = new JobVertexID();
		JobVertexID legacyId2 = new JobVertexID();

		JobVertex jobVertex = new JobVertex("test", defaultId, Arrays.asList(legacyId1, legacyId2), new ArrayList<OperatorID>(), new ArrayList<OperatorID>());
		jobVertex.setInvokableClass(AbstractInvokable.class);

		ExecutionGraph executionGraph = new ExecutionGraph(
			mock(ScheduledExecutorService.class),
			mock(Executor.class),
			new JobID(),
			"test",
			mock(Configuration.class),
			mock(SerializedValue.class),
			Time.seconds(1),
			mock(RestartStrategy.class),
			mock(SlotProvider.class));

		ExecutionJobVertex executionJobVertex =
				new ExecutionJobVertex(executionGraph, jobVertex, 1, Time.seconds(1));

		Map<JobVertexID, ExecutionJobVertex> idToVertex = new HashMap<>();
		idToVertex.put(executionJobVertex.getJobVertexId(), executionJobVertex);

		Assert.assertEquals(executionJobVertex, idToVertex.get(defaultId));
		Assert.assertNull(idToVertex.get(legacyId1));
		Assert.assertNull(idToVertex.get(legacyId2));

		idToVertex = ExecutionJobVertex.includeLegacyJobVertexIDs(idToVertex);

		Assert.assertEquals(3, idToVertex.size());
		Assert.assertEquals(executionJobVertex, idToVertex.get(defaultId));
		Assert.assertEquals(executionJobVertex, idToVertex.get(legacyId1));
		Assert.assertEquals(executionJobVertex, idToVertex.get(legacyId2));
	}
}
