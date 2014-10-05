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

package org.apache.flink.runtime.executiongraph;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.protocols.TaskOperationProtocol;
import org.junit.Test;

public class ExecutionStateProgressTest {

	@Test
	public void testAccumulatedStateFinished() {
		try {
			final JobID jid = new JobID();
			final JobVertexID vid = new JobVertexID();
			
			AbstractJobVertex ajv = new AbstractJobVertex("TestVertex", vid);
			ajv.setParallelism(3);
			ajv.setInvokableClass(mock(AbstractInvokable.class).getClass());
			
			ExecutionGraph graph = new ExecutionGraph(jid, "test job", new Configuration());
			graph.attachJobGraph(Arrays.asList(ajv));
			
			setGraphStatus(graph, JobStatus.RUNNING);
			
			ExecutionJobVertex ejv = graph.getJobVertex(vid);
			
			// mock resources and mock taskmanager
			TaskOperationProtocol taskManager = getSimpleAcknowledgingTaskmanager();
			for (ExecutionVertex ee : ejv.getTaskVertices()) {
				AllocatedSlot slot = getInstance(taskManager).allocateSlot(jid);
				ee.deployToSlot(slot);
			}
			
			// finish all
			for (ExecutionVertex ee : ejv.getTaskVertices()) {
				ee.executionFinished();
			}
			
			assertTrue(ejv.isInFinalState());
			assertEquals(JobStatus.FINISHED, graph.getState());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
