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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.client.AbstractJobResult;
import org.apache.flink.runtime.client.JobSubmissionResult;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.instance.LocalInstanceManager;
import org.apache.flink.runtime.io.network.api.RecordReader;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.io.network.bufferprovider.GlobalBufferPool;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.tasks.AgnosticBinaryReceiver;
import org.apache.flink.runtime.jobmanager.tasks.AgnosticReceiver;
import org.apache.flink.runtime.jobmanager.tasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.jobmanager.tasks.NoOpInvokable;
import org.apache.flink.runtime.jobmanager.tasks.Receiver;
import org.apache.flink.runtime.jobmanager.tasks.Sender;
import org.apache.flink.runtime.types.IntegerRecord;
import org.junit.Test;

import java.util.ArrayList;

/**
 * This test is intended to cover the basic functionality of the {@link JobManager}.
 */
public class JobManagerITCase {
	
	@Test
	public void testScheduleNotEnoughSlots() {
		
		try {
			final AbstractJobVertex vertex = new AbstractJobVertex("Test Vertex");
			vertex.setParallelism(2);
			vertex.setInvokableClass(BlockingNoOpInvokable.class);
			
			final JobGraph jobGraph = new JobGraph("Test Job", vertex);
			
			final JobManager jm = startJobManager(1);
			
			final GlobalBufferPool bp = ((LocalInstanceManager) jm.getInstanceManager())
					.getTaskManagers()[0].getChannelManager().getGlobalBufferPool();
			
			try {
				
				assertEquals(1, jm.getTotalNumberOfRegisteredSlots());
				
				JobSubmissionResult result = jm.submitJob(jobGraph);
				assertEquals(AbstractJobResult.ReturnCode.ERROR, result.getReturnCode());
				
				// monitor the execution
				ExecutionGraph eg = jm.getCurrentJobs().get(jobGraph.getJobID());
				
				if (eg != null) {
					
					long deadline = System.currentTimeMillis() + 60*1000;
					boolean success = false;
					
					while (System.currentTimeMillis() < deadline) {
						JobStatus state = eg.getState();
						if (state == JobStatus.FINISHED) {
							success = true;
							break;
						}
						else if (state == JobStatus.FAILED || state == JobStatus.CANCELED) {
							break;
						}
						else {
							Thread.sleep(200);
						}
					}
					
					assertTrue("The job did not finish successfully.", success);
					
//					assertEquals(0, eg.getRegisteredExecutions().size());
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
	
	@Test
	public void testSingleVertexJobImmediately() {
		
		final int NUM_TASKS = 133;
		
		try {
			final AbstractJobVertex vertex = new AbstractJobVertex("Test Vertex");
			vertex.setParallelism(NUM_TASKS);
			vertex.setInvokableClass(NoOpInvokable.class);
			
			final JobGraph jobGraph = new JobGraph("Test Job", vertex);
			
			final JobManager jm = startJobManager(NUM_TASKS);
			
			final GlobalBufferPool bp = ((LocalInstanceManager) jm.getInstanceManager())
					.getTaskManagers()[0].getChannelManager().getGlobalBufferPool();
			
			try {
				
				assertEquals(NUM_TASKS, jm.getTotalNumberOfRegisteredSlots());
				
				JobSubmissionResult result = jm.submitJob(jobGraph);
				
				if (result.getReturnCode() != AbstractJobResult.ReturnCode.SUCCESS) {
					System.out.println(result.getDescription());
				}
				assertEquals(AbstractJobResult.ReturnCode.SUCCESS, result.getReturnCode());
				
				// monitor the execution
				ExecutionGraph eg = jm.getCurrentJobs().get(jobGraph.getJobID());
				
				if (eg != null) {
					
					long deadline = System.currentTimeMillis() + 60*1000;
					boolean success = false;
					
					while (System.currentTimeMillis() < deadline) {
						JobStatus state = eg.getState();
						if (state == JobStatus.FINISHED) {
							success = true;
							break;
						}
						else if (state == JobStatus.FAILED || state == JobStatus.CANCELED) {
							break;
						}
						else {
							Thread.sleep(200);
						}
					}
					
					assertTrue("The job did not finish successfully.", success);
					
//					assertEquals(0, eg.getRegisteredExecutions().size());
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
	
	@Test
	public void testSingleVertexJobQueued() {
		
		final int NUM_TASKS = 111;
		
		try {
			final AbstractJobVertex vertex = new AbstractJobVertex("Test Vertex");
			vertex.setParallelism(NUM_TASKS);
			vertex.setInvokableClass(NoOpInvokable.class);
			
			final JobGraph jobGraph = new JobGraph("Test Job", vertex);
			jobGraph.setAllowQueuedScheduling(true);
			
			final JobManager jm = startJobManager(10);
			
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
					
//					assertEquals(0, eg.getRegisteredExecutions().size());
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
			
			final JobGraph jobGraph = new JobGraph("Pointwise Job", sender, receiver);
			
			final JobManager jm = startJobManager(2 * NUM_TASKS);
			
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
					
//					assertEquals(0, eg.getRegisteredExecutions().size());
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
	
	@Test
	public void testBipartiteJob() {
		
		final int NUM_TASKS = 31;
		
		try {
			final AbstractJobVertex sender = new AbstractJobVertex("Sender");
			final AbstractJobVertex receiver = new AbstractJobVertex("Receiver");
			
			sender.setInvokableClass(Sender.class);
			receiver.setInvokableClass(AgnosticReceiver.class);
			
			sender.setParallelism(NUM_TASKS);
			receiver.setParallelism(NUM_TASKS);
			
			receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE);
			
			final JobGraph jobGraph = new JobGraph("Bipartite Job", sender, receiver);
			
			final JobManager jm = startJobManager(2 * NUM_TASKS);
			
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
					
//					assertEquals(0, eg.getRegisteredExecutions().size());
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
	
	@Test
	public void testTwoInputJobFailingEdgeMismatch() {
		
		final int NUM_TASKS = 11;
		
		try {
			final AbstractJobVertex sender1 = new AbstractJobVertex("Sender1");
			final AbstractJobVertex sender2 = new AbstractJobVertex("Sender2");
			final AbstractJobVertex receiver = new AbstractJobVertex("Receiver");
			
			sender1.setInvokableClass(Sender.class);
			sender2.setInvokableClass(Sender.class);
			receiver.setInvokableClass(AgnosticReceiver.class);
			
			sender1.setParallelism(NUM_TASKS);
			sender2.setParallelism(2*NUM_TASKS);
			receiver.setParallelism(3*NUM_TASKS);
			
			receiver.connectNewDataSetAsInput(sender1, DistributionPattern.POINTWISE);
			receiver.connectNewDataSetAsInput(sender2, DistributionPattern.BIPARTITE);
			
			final JobGraph jobGraph = new JobGraph("Bipartite Job", sender1, receiver, sender2);
			
			final JobManager jm = startJobManager(6*NUM_TASKS);
			
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
					assertEquals(JobStatus.FAILED, eg.getState());
					
//					assertEquals(0, eg.getRegisteredExecutions().size());
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
			sender2.setParallelism(2*NUM_TASKS);
			receiver.setParallelism(3*NUM_TASKS);
			
			receiver.connectNewDataSetAsInput(sender1, DistributionPattern.POINTWISE);
			receiver.connectNewDataSetAsInput(sender2, DistributionPattern.BIPARTITE);
			
			final JobGraph jobGraph = new JobGraph("Bipartite Job", sender1, receiver, sender2);
			
			JobManager jm = startJobManager(6 * NUM_TASKS);
			
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
					
//					assertEquals(0, eg.getRegisteredExecutions().size());
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
	
	@Test
	public void testJobFailingSender() {
		
		final int NUM_TASKS = 100;
		
		try {
			final AbstractJobVertex sender = new AbstractJobVertex("Sender");
			final AbstractJobVertex receiver = new AbstractJobVertex("Receiver");
			
			sender.setInvokableClass(ExceptionSender.class);
			receiver.setInvokableClass(Receiver.class);
			
			sender.setParallelism(NUM_TASKS);
			receiver.setParallelism(NUM_TASKS);
			
			receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE);
			
			final JobGraph jobGraph = new JobGraph("Pointwise Job", sender, receiver);
			
			final JobManager jm = startJobManager(NUM_TASKS);
			
			final GlobalBufferPool bp = ((LocalInstanceManager) jm.getInstanceManager())
					.getTaskManagers()[0].getChannelManager().getGlobalBufferPool();
			
			try {
				assertEquals(NUM_TASKS, jm.getTotalNumberOfRegisteredSlots());
				
				JobSubmissionResult result = jm.submitJob(jobGraph);

				if (result.getReturnCode() != AbstractJobResult.ReturnCode.SUCCESS) {
					System.out.println(result.getDescription());
				}
				assertEquals(AbstractJobResult.ReturnCode.SUCCESS, result.getReturnCode());
				
				// monitor the execution
				ExecutionGraph eg = jm.getCurrentJobs().get(jobGraph.getJobID());
				
				if (eg != null) {
					eg.waitForJobEnd();
					assertEquals(JobStatus.FAILED, eg.getState());
					
//					assertEquals(0, eg.getRegisteredExecutions().size());
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
	
	@Test
	public void testJobSometimesFailingSender() {
		
		final int NUM_TASKS = 100;
		
		try {
			final AbstractJobVertex sender = new AbstractJobVertex("Sender");
			final AbstractJobVertex receiver = new AbstractJobVertex("Receiver");
			
			sender.setInvokableClass(SometimesExceptionSender.class);
			receiver.setInvokableClass(Receiver.class);
			
			sender.setParallelism(NUM_TASKS);
			receiver.setParallelism(NUM_TASKS);
			
			receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE);
			
			final JobGraph jobGraph = new JobGraph("Pointwise Job", sender, receiver);
			
			final JobManager jm = startJobManager(NUM_TASKS);
			
			final GlobalBufferPool bp = ((LocalInstanceManager) jm.getInstanceManager())
					.getTaskManagers()[0].getChannelManager().getGlobalBufferPool();
			
			try {
				assertEquals(NUM_TASKS, jm.getTotalNumberOfRegisteredSlots());
				
				JobSubmissionResult result = jm.submitJob(jobGraph);

				if (result.getReturnCode() != AbstractJobResult.ReturnCode.SUCCESS) {
					System.out.println(result.getDescription());
				}
				assertEquals(AbstractJobResult.ReturnCode.SUCCESS, result.getReturnCode());
				
				// monitor the execution
				ExecutionGraph eg = jm.getCurrentJobs().get(jobGraph.getJobID());
				
				if (eg != null) {
					eg.waitForJobEnd();
					assertEquals(JobStatus.FAILED, eg.getState());
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
	
	@Test
	public void testJobFailingReceiver() {
		
		final int NUM_TASKS = 200;
		
		try {
			final AbstractJobVertex sender = new AbstractJobVertex("Sender");
			final AbstractJobVertex receiver = new AbstractJobVertex("Receiver");
			
			sender.setInvokableClass(Sender.class);
			receiver.setInvokableClass(ExceptionReceiver.class);
			
			sender.setParallelism(NUM_TASKS);
			receiver.setParallelism(NUM_TASKS);
			
			receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE);
			
			final JobGraph jobGraph = new JobGraph("Pointwise Job", sender, receiver);
			
			final JobManager jm = startJobManager(2 * NUM_TASKS);
			
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
					assertEquals(JobStatus.FAILED, eg.getState());
					
//					assertEquals(0, eg.getRegisteredExecutions().size());
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
	 * Test failure in instantiation, where all fail by themselves
	 */
	@Test
	public void testJobFailingInstantiation() {
		
		final int NUM_TASKS = 200;
		
		try {
			final AbstractJobVertex sender = new AbstractJobVertex("Sender");
			final AbstractJobVertex receiver = new AbstractJobVertex("Receiver");
			
			sender.setInvokableClass(InstantiationErrorSender.class);
			receiver.setInvokableClass(Receiver.class);
			
			sender.setParallelism(NUM_TASKS);
			receiver.setParallelism(NUM_TASKS);
			
			receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE);
			
			final JobGraph jobGraph = new JobGraph("Pointwise Job", sender, receiver);
			
			final JobManager jm = startJobManager(NUM_TASKS);
			
			final GlobalBufferPool bp = ((LocalInstanceManager) jm.getInstanceManager())
					.getTaskManagers()[0].getChannelManager().getGlobalBufferPool();
			
			try {
				assertEquals(NUM_TASKS, jm.getTotalNumberOfRegisteredSlots());
				
				JobSubmissionResult result = jm.submitJob(jobGraph);

				if (result.getReturnCode() != AbstractJobResult.ReturnCode.SUCCESS) {
					System.out.println(result.getDescription());
				}
				assertEquals(AbstractJobResult.ReturnCode.SUCCESS, result.getReturnCode());
				
				// monitor the execution
				ExecutionGraph eg = jm.getCurrentJobs().get(jobGraph.getJobID());
				
				if (eg != null) {
					eg.waitForJobEnd();
					assertEquals(JobStatus.FAILED, eg.getState());
					
//					assertEquals(0, eg.getRegisteredExecutions().size());
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
	 * Test failure in instantiation, where some have to be canceled (not all fail by themselves)
	 */
	@Test
	public void testJobFailingSomeInstantiations() {
		
		final int NUM_TASKS = 200;
		
		try {
			final AbstractJobVertex sender = new AbstractJobVertex("Sender");
			final AbstractJobVertex receiver = new AbstractJobVertex("Receiver");
			
			sender.setInvokableClass(SometimesInstantiationErrorSender.class);
			receiver.setInvokableClass(Receiver.class);
			
			sender.setParallelism(NUM_TASKS);
			receiver.setParallelism(NUM_TASKS);
			
			receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE);
			
			final JobGraph jobGraph = new JobGraph("Pointwise Job", sender, receiver);
			
			final JobManager jm = startJobManager(2*NUM_TASKS);
			
			final GlobalBufferPool bp = ((LocalInstanceManager) jm.getInstanceManager())
					.getTaskManagers()[0].getChannelManager().getGlobalBufferPool();
			
			try {
				assertEquals(2*NUM_TASKS, jm.getNumberOfSlotsAvailableToScheduler());
				
				JobSubmissionResult result = jm.submitJob(jobGraph);

				if (result.getReturnCode() != AbstractJobResult.ReturnCode.SUCCESS) {
					System.out.println(result.getDescription());
				}
				assertEquals(AbstractJobResult.ReturnCode.SUCCESS, result.getReturnCode());
				
				// monitor the execution
				ExecutionGraph eg = jm.getCurrentJobs().get(jobGraph.getJobID());
				
				if (eg != null) {
					eg.waitForJobEnd();
					assertEquals(JobStatus.FAILED, eg.getState());
					
//					assertEquals(0, eg.getRegisteredExecutions().size());
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
	
	// --------------------------------------------------------------------------------------------
	//  Simple test tasks
	// --------------------------------------------------------------------------------------------
	
	public static final class ExceptionSender extends AbstractInvokable {

		private RecordWriter<IntegerRecord> writer;
		
		@Override
		public void registerInputOutput() {
			writer = new RecordWriter<IntegerRecord>(this);
		}

		@Override
		public void invoke() throws Exception {
			writer.initializeSerializers();
			
			throw new Exception("Test Exception");
		}
	}
	
	public static final class ExceptionReceiver extends AbstractInvokable {
		
		@Override
		public void registerInputOutput() {
			new RecordReader<IntegerRecord>(this, IntegerRecord.class);
		}

		@Override
		public void invoke() throws Exception {
			throw new Exception("Expected Test Exception");
		}
	}
	
	public static final class SometimesExceptionSender extends AbstractInvokable {

		private RecordWriter<IntegerRecord> writer;
		
		@Override
		public void registerInputOutput() {
			writer = new RecordWriter<IntegerRecord>(this);
		}

		@Override
		public void invoke() throws Exception {
			writer.initializeSerializers();
			
			if (Math.random() < 0.05) {
				throw new Exception("Test Exception");
			} else {
				Object o = new Object();
				synchronized (o) {
					o.wait();
				}
			}
		}
	}
	
	public static final class InstantiationErrorSender extends AbstractInvokable {

		public InstantiationErrorSender() {
			throw new RuntimeException("Test Exception in Constructior");
		}
		
		@Override
		public void registerInputOutput() {}

		@Override
		public void invoke() {}
	}
	
	public static final class SometimesInstantiationErrorSender extends AbstractInvokable {
		
		public SometimesInstantiationErrorSender() {
			if (Math.random() < 0.05) {
				throw new RuntimeException("Test Exception in Constructior");
			}
		}
		
		@Override
		public void registerInputOutput() {
			new RecordWriter<IntegerRecord>(this);
		}
		
		@Override
		public void invoke() throws Exception {
			Object o = new Object();
			synchronized (o) {
				o.wait();
			}
		}
	}
}
