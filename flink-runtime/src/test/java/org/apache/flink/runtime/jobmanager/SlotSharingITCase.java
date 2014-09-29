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

import static org.apache.flink.runtime.jobgraph.JobManagerTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.client.AbstractJobResult;
import org.apache.flink.runtime.client.JobSubmissionResult;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.instance.LocalInstanceManager;
import org.apache.flink.runtime.io.network.bufferprovider.GlobalBufferPool;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmanager.tasks.AgnosticBinaryReceiver;
import org.apache.flink.runtime.jobmanager.tasks.Receiver;
import org.apache.flink.runtime.jobmanager.tasks.Sender;
import org.junit.Test;

import java.util.ArrayList;

public class SlotSharingITCase {

	
	/**
	 * This job runs in N slots with N senders and N receivers. Unless slot sharing is used, it cannot complete.
	 */
	@Test
	public void testForwardJob() {
		
		final int NUM_TASKS = 31;
		
		try {
			final AbstractJobVertex sender = new AbstractJobVertex("Sender");
			final AbstractJobVertex receiver = new AbstractJobVertex("Receiver");
			
			sender.setInvokableClass(Sender.class);
			receiver.setInvokableClass(Receiver.class);
			
			sender.setParallelism(NUM_TASKS);
			receiver.setParallelism(NUM_TASKS);
			
			receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE);
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup(sender.getID(), receiver.getID());
			sender.setSlotSharingGroup(sharingGroup);
			receiver.setSlotSharingGroup(sharingGroup);
			
			final JobGraph jobGraph = new JobGraph("Pointwise Job", sender, receiver);
			
			final JobManager jm = startJobManager(NUM_TASKS);
			
			final GlobalBufferPool bp = ((LocalInstanceManager) jm.getInstanceManager())
					.getTaskManagers()[0].getChannelManager().getGlobalBufferPool();
			
			try {
				JobSubmissionResult result = jm.submitJob(jobGraph);

				if (result.getReturnCode() != AbstractJobResult.ReturnCode.SUCCESS) {
					System.out.println(result.getDescription());
				}
				assertEquals(AbstractJobResult.ReturnCode.SUCCESS, result.getReturnCode());
				
				// monitor the execution
				ExecutionGraph eg = jm.getCurrentJobs().get(jobGraph.getJobID());
				
				if (eg != null) {
					eg.waitForJobEnd();
					assertEquals(JobStatus.FINISHED, eg.getState());
				}
				else {
					// already done, that was fast;
				}
				
				// make sure that in any case, the network buffers are all returned
				waitForTaskThreadsToBeTerminated();
				assertEquals(bp.numBuffers(), bp.numAvailableBuffers());
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
	
	/**
	 * This job runs in N slots with 2 * N senders and N receivers. Unless slot sharing is used, it cannot complete.
	 */
	@Test
	public void testTwoInputJob() {
		
		final int NUM_TASKS = 11;
		
		try {
			final AbstractJobVertex sender1 = new AbstractJobVertex("Sender1");
			final AbstractJobVertex sender2 = new AbstractJobVertex("Sender2");
			final AbstractJobVertex receiver = new AbstractJobVertex("Receiver");
			
			sender1.setInvokableClass(Sender.class);
			sender2.setInvokableClass(Sender.class);
			receiver.setInvokableClass(AgnosticBinaryReceiver.class);
			
			sender1.setParallelism(NUM_TASKS);
			sender2.setParallelism(NUM_TASKS);
			receiver.setParallelism(NUM_TASKS);
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup(sender1.getID(), sender2.getID(), receiver.getID());
			sender1.setSlotSharingGroup(sharingGroup);
			sender2.setSlotSharingGroup(sharingGroup);
			receiver.setSlotSharingGroup(sharingGroup);;
			
			receiver.connectNewDataSetAsInput(sender1, DistributionPattern.POINTWISE);
			receiver.connectNewDataSetAsInput(sender2, DistributionPattern.BIPARTITE);
			
			final JobGraph jobGraph = new JobGraph("Bipartite Job", sender1, receiver, sender2);
			
			JobManager jm = startJobManager(NUM_TASKS);
			
			final GlobalBufferPool bp = ((LocalInstanceManager) jm.getInstanceManager())
								.getTaskManagers()[0].getChannelManager().getGlobalBufferPool();
			
			try {
				JobSubmissionResult result = jm.submitJob(jobGraph);

				if (result.getReturnCode() != AbstractJobResult.ReturnCode.SUCCESS) {
					System.out.println(result.getDescription());
				}
				assertEquals(AbstractJobResult.ReturnCode.SUCCESS, result.getReturnCode());
				
				// monitor the execution
				ExecutionGraph eg = jm.getCurrentJobs().get(jobGraph.getJobID());
				
				if (eg != null) {
					eg.waitForJobEnd();
					assertEquals(JobStatus.FINISHED, eg.getState());
				}
				else {
					// already done, that was fast;
				}
				
				// make sure that in any case, the network buffers are all returned
				waitForTaskThreadsToBeTerminated();
				assertEquals(bp.numBuffers(), bp.numAvailableBuffers());
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
