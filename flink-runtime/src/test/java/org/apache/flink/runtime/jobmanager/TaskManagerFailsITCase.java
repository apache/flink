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

package org.apache.flink.runtime.jobmanager;

import static org.apache.flink.runtime.jobgraph.JobManagerTestUtils.startJobManager;
import static org.apache.flink.runtime.jobgraph.JobManagerTestUtils.waitForTaskThreadsToBeTerminated;
import static org.junit.Assert.*;

import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.client.AbstractJobResult;
import org.apache.flink.runtime.client.JobSubmissionResult;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.LocalInstanceManager;
import org.apache.flink.runtime.io.network.bufferprovider.GlobalBufferPool;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.tasks.BlockingReceiver;
import org.apache.flink.runtime.jobmanager.tasks.Sender;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.junit.Test;

import java.util.ArrayList;

public class TaskManagerFailsITCase {

	@Test
	public void testExecutionWithFailingTaskManager() {
		final int NUM_TASKS = 31;
		
		try {
			final AbstractJobVertex sender = new AbstractJobVertex("Sender");
			final AbstractJobVertex receiver = new AbstractJobVertex("Receiver");
			sender.setInvokableClass(Sender.class);
			receiver.setInvokableClass(BlockingReceiver.class);
			sender.setParallelism(NUM_TASKS);
			receiver.setParallelism(NUM_TASKS);
			receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE);
			
			final JobGraph jobGraph = new JobGraph("Pointwise Job", sender, receiver);
			
			final JobManager jm = startJobManager(2, NUM_TASKS);
			
			final TaskManager tm1 = ((LocalInstanceManager) jm.getInstanceManager()).getTaskManagers()[0];
			final TaskManager tm2 = ((LocalInstanceManager) jm.getInstanceManager()).getTaskManagers()[1];
			
			final GlobalBufferPool bp1 = tm1.getChannelManager().getGlobalBufferPool();
			final GlobalBufferPool bp2 = tm2.getChannelManager().getGlobalBufferPool();
			
			try {

				JobSubmissionResult result = jm.submitJob(jobGraph);

				if (result.getReturnCode() != AbstractJobResult.ReturnCode.SUCCESS) {
					System.out.println(result.getDescription());
				}
				assertEquals(AbstractJobResult.ReturnCode.SUCCESS, result.getReturnCode());
				
				ExecutionGraph eg = jm.getCurrentJobs().get(jobGraph.getJobID());
				
				// wait until everyone has settled in
				long deadline = System.currentTimeMillis() + 2000;
				while (System.currentTimeMillis() < deadline) {
					
					boolean allrunning = true;
					for (ExecutionVertex v : eg.getJobVertex(receiver.getID()).getTaskVertices()) {
						if (v.getCurrentExecutionAttempt().getState() != ExecutionState.RUNNING) {
							allrunning = false;
							break;
						}
					}
					
					if (allrunning) {
						break;
					}
					Thread.sleep(200);
				}
				
				// kill one task manager
				TaskManager tm = ((LocalInstanceManager) jm.getInstanceManager()).getTaskManagers()[0];
				tm.shutdown();
				
				eg.waitForJobEnd();
				
				// make sure that in any case, the network buffers are all returned
				waitForTaskThreadsToBeTerminated();
				assertTrue(bp1.isDestroyed() || bp1.numBuffers() == bp1.numAvailableBuffers());
				assertTrue(bp2.isDestroyed() || bp2.numBuffers() == bp2.numAvailableBuffers());
			}
			finally {
				jm.shutdown();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
