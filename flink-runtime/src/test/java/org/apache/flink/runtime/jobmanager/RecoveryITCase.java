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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.AbstractJobResult;
import org.apache.flink.runtime.client.JobSubmissionResult;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.instance.LocalInstanceManager;
import org.apache.flink.runtime.io.network.bufferprovider.GlobalBufferPool;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmanager.tasks.ReceiverBlockingOnce;
import org.apache.flink.runtime.jobmanager.tasks.ReceiverFailingOnce;
import org.apache.flink.runtime.jobmanager.tasks.Sender;
import org.junit.Test;

/**
 * This test is intended to cover the basic functionality of the {@link JobManager}.
 */
public class RecoveryITCase {
	
	@Test
	public void testForwardJob() {
		
		ReceiverFailingOnce.resetFailedBefore();
		
		final int NUM_TASKS = 31;
		
		JobManager jm = null;
		
		try {
			final AbstractJobVertex sender = new AbstractJobVertex("Sender");
			final AbstractJobVertex receiver = new AbstractJobVertex("Receiver");
			
			sender.setInvokableClass(Sender.class);
			receiver.setInvokableClass(ReceiverFailingOnce.class);
			
			sender.setParallelism(NUM_TASKS);
			receiver.setParallelism(NUM_TASKS);
			
			receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE);
			
			final JobGraph jobGraph = new JobGraph("Pointwise Job", sender, receiver);
			jobGraph.setNumberOfExecutionRetries(1);
			
			jm = startJobManager(2 * NUM_TASKS);
			
			final GlobalBufferPool bp = ((LocalInstanceManager) jm.getInstanceManager())
					.getTaskManagers()[0].getChannelManager().getGlobalBufferPool();
			
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
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (jm != null) {
				jm.shutdown();
			}
		}
	}
	
	@Test
	public void testForwardJobWithSlotSharing() {
		
		ReceiverFailingOnce.resetFailedBefore();
		
		final int NUM_TASKS = 31;
		
		JobManager jm = null;
		
		try {
			final AbstractJobVertex sender = new AbstractJobVertex("Sender");
			final AbstractJobVertex receiver = new AbstractJobVertex("Receiver");
			
			sender.setInvokableClass(Sender.class);
			receiver.setInvokableClass(ReceiverFailingOnce.class);
			
			sender.setParallelism(NUM_TASKS);
			receiver.setParallelism(NUM_TASKS);
			
			receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE);
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup();
			sender.setSlotSharingGroup(sharingGroup);
			receiver.setSlotSharingGroup(sharingGroup);
			
			final JobGraph jobGraph = new JobGraph("Pointwise Job", sender, receiver);
			jobGraph.setNumberOfExecutionRetries(1);
			
			jm = startJobManager(NUM_TASKS);
			
			final GlobalBufferPool bp = ((LocalInstanceManager) jm.getInstanceManager())
					.getTaskManagers()[0].getChannelManager().getGlobalBufferPool();
			
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
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (jm != null) {
				jm.shutdown();
			}
		}
	}
	
	@Test
	public void testRecoverTaskManagerFailure() {
		
		final int NUM_TASKS = 31;
		
		JobManager jm = null;
		
		try {
			final AbstractJobVertex sender = new AbstractJobVertex("Sender");
			final AbstractJobVertex receiver = new AbstractJobVertex("Receiver");
			
			sender.setInvokableClass(Sender.class);
			receiver.setInvokableClass(ReceiverBlockingOnce.class);
			sender.setParallelism(NUM_TASKS);
			receiver.setParallelism(NUM_TASKS);
			
			receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE);
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup();
			sender.setSlotSharingGroup(sharingGroup);
			receiver.setSlotSharingGroup(sharingGroup);
			
			final JobGraph jobGraph = new JobGraph("Pointwise Job", sender, receiver);
			jobGraph.setNumberOfExecutionRetries(1);
			
			// make sure we have fast heartbeats and failure detection
			Configuration cfg = new Configuration();
			cfg.setInteger(ConfigConstants.JOB_MANAGER_DEAD_TASKMANAGER_TIMEOUT_KEY, 3000);
			cfg.setInteger(ConfigConstants.TASK_MANAGER_HEARTBEAT_INTERVAL_KEY, 1000);
			
			jm = startJobManager(2, NUM_TASKS, cfg);
			
			JobSubmissionResult result = jm.submitJob(jobGraph);

			if (result.getReturnCode() != AbstractJobResult.ReturnCode.SUCCESS) {
				System.out.println(result.getDescription());
			}
			assertEquals(AbstractJobResult.ReturnCode.SUCCESS, result.getReturnCode());
			
			// monitor the execution
			ExecutionGraph eg = jm.getCurrentJobs().get(jobGraph.getJobID());
			
			// wait for a bit until all is running, make sure the second attempt does not block
			Thread.sleep(300);
			ReceiverBlockingOnce.setShouldNotBlock();
			
			// shutdown one of the taskmanagers
			((LocalInstanceManager) jm.getInstanceManager()).getTaskManagers()[0].shutdown();
			
			// wait for the recovery to do its work
			if (eg != null) {
				eg.waitForJobEnd();
				assertEquals(JobStatus.FINISHED, eg.getState());
			}
			else {
				// already done, that was fast;
			}
			
			// make sure that in any case, the network buffers are all returned
			waitForTaskThreadsToBeTerminated();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (jm != null) {
				jm.shutdown();
			}
		}
	}
}
