/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.executiongraph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceException;
import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceRequestMap;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.InstanceTypeFactory;
import eu.stratosphere.nephele.jobgraph.DistributionPattern;
import eu.stratosphere.runtime.io.channels.ChannelType;
import eu.stratosphere.nephele.util.FileLineReader;
import eu.stratosphere.nephele.util.FileLineWriter;
import eu.stratosphere.nephele.jobgraph.JobFileInputVertex;
import eu.stratosphere.nephele.jobgraph.JobFileOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.topology.NetworkTopology;
import eu.stratosphere.nephele.util.ServerTestUtils;
import eu.stratosphere.util.LogUtils;

/**
 * This class contains test concerning the correct conversion from {@link JobGraph} to {@link ExecutionGraph} objects.
 * 
 */
public class ExecutionGraphTest {

	/**
	 * The name of the default instance type used during these tests.
	 */
	private static final String DEFAULT_INSTANCE_TYPE_NAME = "test";

	/**
	 * A test implementation of an {@link InstanceManager} which is used as a stub in these tests.
	 * 
	 */
	private static final class TestInstanceManager implements InstanceManager {

		/**
		 * The default instance type.
		 */
		private final InstanceType defaultInstanceType;

		/**
		 * Constructs a new test instance manager.
		 */
		public TestInstanceManager() {
			this.defaultInstanceType = InstanceTypeFactory.construct(DEFAULT_INSTANCE_TYPE_NAME, 4, 4, 1024, 50, 10);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void requestInstance(final JobID jobID, final Configuration conf,
				final InstanceRequestMap instanceRequestMap,
				final List<String> splitAffinityList) throws InstanceException {

			throw new IllegalStateException("requestInstance called on TestInstanceManager");
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void releaseAllocatedResource(final JobID jobID, final Configuration conf,
				final AllocatedResource allocatedResource)
				throws InstanceException {

			throw new IllegalStateException("releaseAllocatedResource called on TestInstanceManager");
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public InstanceType getSuitableInstanceType(final int minNumComputeUnits, final int minNumCPUCores,
				final int minMemorySize, final int minDiskCapacity, final int maxPricePerHour) {

			throw new IllegalStateException("getSuitableInstanceType called on TestInstanceManager");
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void reportHeartBeat(final InstanceConnectionInfo instanceConnectionInfo,
				final HardwareDescription hardwareDescription) {

			throw new IllegalStateException("reportHeartBeat called on TestInstanceManager");
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public InstanceType getInstanceTypeByName(final String instanceTypeName) {

			if (this.defaultInstanceType.getIdentifier().equals(instanceTypeName)) {
				return this.defaultInstanceType;
			}

			return null;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public InstanceType getDefaultInstanceType() {

			return this.defaultInstanceType;
		}

		@Override
		public NetworkTopology getNetworkTopology(final JobID jobID) {

			throw new IllegalStateException("getNetworkTopology called on TestInstanceManager");
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void setInstanceListener(final InstanceListener instanceListener) {

			throw new IllegalStateException("setInstanceListener called on TestInstanceManager");
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public Map<InstanceType, InstanceTypeDescription> getMapOfAvailableInstanceTypes() {

			throw new IllegalStateException("getMapOfAvailableInstanceType called on TestInstanceManager");
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void shutdown() {

			throw new IllegalStateException("shutdown called on TestInstanceManager");
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public AbstractInstance getInstanceByName(final String name) {
			throw new IllegalStateException("getInstanceByName called on TestInstanceManager");
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void cancelPendingRequests(final JobID jobID) {
			throw new IllegalStateException("cancelPendingRequests called on TestInstanceManager");
		}

		@Override
		public int getNumberOfTaskTrackers() {
			return 0;
		}

	}

	private static final InstanceManager INSTANCE_MANAGER = new TestInstanceManager();

	@BeforeClass
	public static void reduceLogLevel() {
		LogUtils.initializeDefaultConsoleLogger(Level.WARN);
	}
	
	/*
	 * input1 -> task1 -> output1
	 * output1 shares instance with input1
	 * input1 shares instance with task1
	 * no subtasks defined
	 * input1 is default, task1 is m1.large, output1 is m1.xlarge
	 * no channel types defined
	 */
	@Test
	public void testConvertJobGraphToExecutionGraph1() {

		File inputFile = null;
		JobID jobID = null;

		try {
			inputFile = ServerTestUtils.createInputFile(0);

			// create job graph
			final JobGraph jg = new JobGraph("Job Graph 1");
			jobID = jg.getJobID();

			// input vertex
			final JobFileInputVertex i1 = new JobFileInputVertex("Input 1", jg);
			i1.setFileInputClass(FileLineReader.class);
			i1.setFilePath(new Path(inputFile.toURI()));

			// task vertex
			final JobTaskVertex t1 = new JobTaskVertex("Task 1", jg);
			t1.setTaskClass(ForwardTask1Input1Output.class);

			// output vertex
			final JobFileOutputVertex o1 = new JobFileOutputVertex("Output 1", jg);
			o1.setFileOutputClass(FileLineWriter.class);
			o1.setFilePath(new Path(new File(ServerTestUtils.getRandomFilename()).toURI()));

			o1.setVertexToShareInstancesWith(i1);
			i1.setVertexToShareInstancesWith(t1);

			// connect vertices
			i1.connectTo(t1);
			t1.connectTo(o1);

			LibraryCacheManager.register(jobID, new String[0]);

			final ExecutionGraph eg = new ExecutionGraph(jg, INSTANCE_MANAGER);

			// test all methods of ExecutionGraph
			final InstanceRequestMap instanceRequestMap = new InstanceRequestMap();
			final ExecutionStage executionStage = eg.getCurrentExecutionStage();
			executionStage.collectRequiredInstanceTypes(instanceRequestMap, ExecutionState.CREATED);
			assertEquals(1, instanceRequestMap.size());
			assertEquals(1, (int) instanceRequestMap.getMaximumNumberOfInstances(INSTANCE_MANAGER
					.getInstanceTypeByName(DEFAULT_INSTANCE_TYPE_NAME)));

			assertEquals(jobID, eg.getJobID());
			assertEquals(0, eg.getIndexOfCurrentExecutionStage());
			assertEquals(1, eg.getNumberOfInputVertices());
			assertEquals(1, eg.getNumberOfOutputVertices());
			assertEquals(1, eg.getNumberOfStages());
			assertNotNull(eg.getInputVertex(0));
			assertNull(eg.getInputVertex(1));
			assertNotNull(eg.getOutputVertex(0));
			assertNull(eg.getOutputVertex(1));
			assertNotNull(eg.getStage(0));
			assertNull(eg.getStage(1));

			// test all methods of ExecutionStage stage0
			ExecutionStage es = eg.getStage(0);

			assertEquals(3, es.getNumberOfStageMembers());
			assertEquals(0, es.getStageNumber());
			assertNotNull(es.getStageMember(0));
			assertNotNull(es.getStageMember(1));
			assertNotNull(es.getStageMember(2));
			assertNull(es.getStageMember(3));

			// test all methods of ExecutionGroupVertex
			ExecutionGroupVertex egv0 = null; // input1
			ExecutionGroupVertex egv1 = null; // output1
			ExecutionGroupVertex egv2 = null; // task1

			if (es.getStageMember(0).getName().equals("Input 1")) {
				egv0 = es.getStageMember(0);
			} else if (es.getStageMember(0).getName().equals("Output 1")) {
				egv1 = es.getStageMember(0);
			} else {
				egv2 = es.getStageMember(0);
			}

			if (es.getStageMember(1).getName().equals("Input 1")) {
				egv0 = es.getStageMember(1);
			} else if (es.getStageMember(1).getName().equals("Output 1")) {
				egv1 = es.getStageMember(1);
			} else {
				egv2 = es.getStageMember(1);
			}

			if (es.getStageMember(2).getName().equals("Input 1")) {
				egv0 = es.getStageMember(2);
			} else if (es.getStageMember(2).getName().equals("Output 1")) {
				egv1 = es.getStageMember(2);
			} else {
				egv2 = es.getStageMember(2);
			}

			// egv0 (input1)
			assertNull(egv0.getBackwardEdge(0));
			assertNotNull(egv0.getConfiguration());
			assertEquals(1, egv0.getCurrentNumberOfGroupMembers());
			assertNotNull(egv0.getExecutionSignature());
			assertEquals(es, egv0.getExecutionStage());
			assertNotNull(egv0.getForwardEdge(0));
			assertNull(egv0.getForwardEdge(1));
			assertNotNull(egv0.getForwardEdges(egv2));
			assertNotNull(egv0.getGroupMember(0));
			assertNull(egv0.getGroupMember(1));
			assertEquals(1, egv0.getInputSplits().length);
			assertEquals(-1, egv0.getMaximumNumberOfGroupMembers());
			assertEquals(1, egv0.getMinimumNumberOfGroupMember());
			assertEquals("Input 1", egv0.getName());
			assertEquals(0, egv0.getNumberOfBackwardLinks());
			assertEquals(1, egv0.getNumberOfForwardLinks());
			assertEquals(1, egv0.getNumberOfSubtasksPerInstance());
			assertEquals(0, egv0.getStageNumber());
			assertEquals(-1, egv0.getUserDefinedNumberOfMembers());
			assertEquals(INSTANCE_MANAGER.getDefaultInstanceType(), egv0.getInstanceType());
			assertEquals("Task 1", egv0.getVertexToShareInstancesWith().getName());

			// egv1 (output1)
			assertNotNull(egv1.getBackwardEdge(0));
			assertNull(egv1.getBackwardEdge(1));
			assertNotNull(egv1.getBackwardEdges(egv2));
			assertNotNull(egv1.getConfiguration());
			assertEquals(1, egv1.getCurrentNumberOfGroupMembers());
			assertNotNull(egv1.getExecutionSignature());
			assertEquals(es, egv1.getExecutionStage());
			assertNull(egv1.getForwardEdge(0));
			assertNotNull(egv1.getGroupMember(0));
			assertNull(egv1.getGroupMember(1));
			assertEquals(1, egv1.getMaximumNumberOfGroupMembers());
			assertEquals(1, egv1.getMinimumNumberOfGroupMember());
			assertEquals("Output 1", egv1.getName());
			assertEquals(1, egv1.getNumberOfBackwardLinks());
			assertEquals(0, egv1.getNumberOfForwardLinks());
			assertEquals(1, egv1.getNumberOfSubtasksPerInstance());
			assertEquals(0, egv1.getStageNumber());
			assertEquals(-1, egv1.getUserDefinedNumberOfMembers());
			assertEquals(INSTANCE_MANAGER.getInstanceTypeByName(DEFAULT_INSTANCE_TYPE_NAME), egv1.getInstanceType());
			assertEquals("Input 1", egv1.getVertexToShareInstancesWith().getName());

			// egv2 (task1)
			assertNotNull(egv2.getBackwardEdge(0));
			assertNull(egv2.getBackwardEdge(1));
			assertNotNull(egv2.getBackwardEdges(egv0));
			assertNotNull(egv2.getConfiguration());
			assertEquals(1, egv2.getCurrentNumberOfGroupMembers());
			assertNotNull(egv2.getExecutionSignature());
			assertEquals(es, egv2.getExecutionStage());
			assertNotNull(egv2.getForwardEdge(0));
			assertNull(egv2.getForwardEdge(1));
			assertNotNull(egv2.getForwardEdges(egv1));
			assertNotNull(egv2.getGroupMember(0));
			assertNull(egv2.getGroupMember(1));
			assertEquals(-1, egv2.getMaximumNumberOfGroupMembers());
			assertEquals(1, egv2.getMinimumNumberOfGroupMember());
			assertEquals("Task 1", egv2.getName());
			assertEquals(1, egv2.getNumberOfBackwardLinks());
			assertEquals(1, egv2.getNumberOfForwardLinks());
			assertEquals(1, egv2.getNumberOfSubtasksPerInstance());
			assertEquals(0, egv2.getStageNumber());
			assertEquals(-1, egv2.getUserDefinedNumberOfMembers());
			assertEquals(INSTANCE_MANAGER.getInstanceTypeByName(DEFAULT_INSTANCE_TYPE_NAME), egv2.getInstanceType());
			assertNull(egv2.getVertexToShareInstancesWith());

			// test all methods of ExecutionVertex
			ExecutionVertex ev0 = egv0.getGroupMember(0); // input1
			ExecutionVertex ev1 = egv1.getGroupMember(0); // output1
			ExecutionVertex ev2 = egv2.getGroupMember(0); // task1

			// ev0 (input1)
			assertEquals(egv0, ev0.getGroupVertex());
			assertNotNull(ev0.getID());
			assertEquals("Input 1", ev0.getName());
			assertEquals(INSTANCE_MANAGER.getInstanceTypeByName(DEFAULT_INSTANCE_TYPE_NAME), ev0.getAllocatedResource()
				.getInstance()
				.getType());

			// ev1 (output1)
			assertEquals(egv1, ev1.getGroupVertex());
			assertNotNull(ev1.getID());
			assertEquals("Output 1", ev1.getName());
			assertEquals(INSTANCE_MANAGER.getInstanceTypeByName(DEFAULT_INSTANCE_TYPE_NAME), ev1.getAllocatedResource()
				.getInstance()
				.getType());

			// ev2 (task1)
			assertEquals(egv2, ev2.getGroupVertex());
			assertNotNull(ev2.getID());
			assertEquals("Task 1", ev2.getName());
			assertEquals(INSTANCE_MANAGER.getInstanceTypeByName(DEFAULT_INSTANCE_TYPE_NAME), ev2.getAllocatedResource()
				.getInstance()
				.getType());

			assertEquals(ev0.getAllocatedResource(), ev1.getAllocatedResource());
			assertEquals(ev0.getAllocatedResource(), ev2.getAllocatedResource());

			// test channels
			assertEquals(ChannelType.NETWORK, eg.getChannelType(ev0, ev2));
			assertEquals(ChannelType.NETWORK, eg.getChannelType(ev2, ev1));

		} catch (GraphConversionException e) {
			fail(e.getMessage());
		} catch (JobGraphDefinitionException e) {
			fail(e.getMessage());
		} catch (IOException e) {
			fail(e.getMessage());
		} finally {
			if (inputFile != null) {
				inputFile.delete();
			}
			if (jobID != null) {
				try {
					LibraryCacheManager.unregister(jobID);
				} catch (IOException e) {
				}
			}
		}
	}

	/*
	 * input1 -> task1 -> output1
	 * no subtasks defined
	 * input1 is default, task1 is m1.large, output1 is m1.xlarge
	 * all channels are IN_MEMORY
	 */
	@Test
	public void testConvertJobGraphToExecutionGraph2() {

		File inputFile = null;
		JobID jobID = null;

		try {
			inputFile = ServerTestUtils.createInputFile(0);

			// create job graph
			final JobGraph jg = new JobGraph("Job Graph 1");
			jobID = jg.getJobID();

			// input vertex
			final JobFileInputVertex i1 = new JobFileInputVertex("Input 1", jg);
			i1.setFileInputClass(FileLineReader.class);
			i1.setFilePath(new Path(inputFile.toURI()));

			// task vertex
			final JobTaskVertex t1 = new JobTaskVertex("Task 1", jg);
			t1.setTaskClass(ForwardTask1Input1Output.class);

			// output vertex
			final JobFileOutputVertex o1 = new JobFileOutputVertex("Output 1", jg);
			o1.setFileOutputClass(FileLineWriter.class);
			o1.setFilePath(new Path(new File(ServerTestUtils.getRandomFilename()).toURI()));

			// connect vertices
			i1.connectTo(t1, ChannelType.IN_MEMORY);
			t1.connectTo(o1, ChannelType.IN_MEMORY);

			LibraryCacheManager.register(jobID, new String[0]);

			// now convert job graph to execution graph
			final ExecutionGraph eg = new ExecutionGraph(jg, INSTANCE_MANAGER);

			// test instance types in ExecutionGraph
			final InstanceRequestMap instanceRequestMap = new InstanceRequestMap();
			final ExecutionStage executionStage = eg.getCurrentExecutionStage();
			executionStage.collectRequiredInstanceTypes(instanceRequestMap, ExecutionState.CREATED);
			assertEquals(1, instanceRequestMap.size());
			assertEquals(1,
				(int) instanceRequestMap.getMaximumNumberOfInstances(INSTANCE_MANAGER.getDefaultInstanceType()));

			// stage0
			ExecutionStage es = eg.getStage(0);
			ExecutionGroupVertex egv0 = null; // input1
			ExecutionGroupVertex egv1 = null; // output1
			ExecutionGroupVertex egv2 = null; // task1
			if (es.getStageMember(0).getName().equals("Input 1")) {
				egv0 = es.getStageMember(0);
			} else if (es.getStageMember(0).getName().equals("Output 1")) {
				egv1 = es.getStageMember(0);
			} else {
				egv2 = es.getStageMember(0);
			}
			if (es.getStageMember(1).getName().equals("Input 1")) {
				egv0 = es.getStageMember(1);
			} else if (es.getStageMember(1).getName().equals("Output 1")) {
				egv1 = es.getStageMember(1);
			} else {
				egv2 = es.getStageMember(1);
			}
			if (es.getStageMember(2).getName().equals("Input 1")) {
				egv0 = es.getStageMember(2);
			} else if (es.getStageMember(2).getName().equals("Output 1")) {
				egv1 = es.getStageMember(2);
			} else {
				egv2 = es.getStageMember(2);
			}
			ExecutionVertex ev0 = egv0.getGroupMember(0); // input1
			ExecutionVertex ev1 = egv1.getGroupMember(0); // output1
			ExecutionVertex ev2 = egv2.getGroupMember(0); // task1
			// ev0 (input1)
			assertEquals(INSTANCE_MANAGER.getDefaultInstanceType(), ev0.getAllocatedResource().getInstance().getType());
			// ev1 (output1)
			assertEquals(INSTANCE_MANAGER.getDefaultInstanceType(), ev1.getAllocatedResource().getInstance().getType());
			// ev2 (task1)
			assertEquals(INSTANCE_MANAGER.getDefaultInstanceType(), ev2.getAllocatedResource().getInstance().getType());
			assertEquals(ev0.getAllocatedResource(), ev1.getAllocatedResource());
			assertEquals(ev0.getAllocatedResource(), ev2.getAllocatedResource());
		} catch (GraphConversionException e) {
			fail(e.getMessage());
		} catch (IOException e) {
			fail(e.getMessage());
		} catch (JobGraphDefinitionException e) {
			fail(e.getMessage());
		} finally {
			if (inputFile != null) {
				inputFile.delete();
			}
			if (jobID != null) {
				try {
					LibraryCacheManager.unregister(jobID);
				} catch (IOException e) {
				}
			}
		}
	}

	/*
	 * input1 -> task1 ->
	 * task3 -> output1
	 * input2 -> task2 ->
	 * each vertex has 2 subtasks
	 * no instance types defined
	 * no channel types defined
	 */
	@Test
	public void testConvertJobGraphToExecutionGraph3() {

		File inputFile1 = null;
		File inputFile2 = null;
		File outputFile = null;
		JobID jobID = null;

		try {

			inputFile1 = ServerTestUtils.createInputFile(0);
			inputFile2 = ServerTestUtils.createInputFile(0);
			outputFile = new File(ServerTestUtils.getRandomFilename());

			// create job graph
			final JobGraph jg = new JobGraph("Job Graph 1");
			jobID = jg.getJobID();

			// input vertex
			final JobFileInputVertex i1 = new JobFileInputVertex("Input 1", jg);
			i1.setFileInputClass(FileLineReader.class);
			i1.setFilePath(new Path(inputFile1.toURI()));
			i1.setNumberOfSubtasks(2);
			final JobFileInputVertex i2 = new JobFileInputVertex("Input 2", jg);
			i2.setFileInputClass(FileLineReader.class);
			i2.setFilePath(new Path(inputFile2.toURI()));
			i2.setNumberOfSubtasks(2);

			// task vertex
			final JobTaskVertex t1 = new JobTaskVertex("Task 1", jg);
			t1.setTaskClass(ForwardTask1Input1Output.class);
			t1.setNumberOfSubtasks(2);
			final JobTaskVertex t2 = new JobTaskVertex("Task 2", jg);
			t2.setTaskClass(ForwardTask1Input1Output.class);
			t2.setNumberOfSubtasks(2);
			final JobTaskVertex t3 = new JobTaskVertex("Task 3", jg);
			t3.setTaskClass(ForwardTask2Inputs1Output.class);
			t3.setNumberOfSubtasks(2);

			
			// output vertex
			final JobFileOutputVertex o1 = new JobFileOutputVertex("Output 1", jg);
			o1.setFileOutputClass(FileLineWriter.class);
			o1.setFilePath(new Path(outputFile.toURI()));
			o1.setNumberOfSubtasks(2);
			i1.setVertexToShareInstancesWith(t1);
			t1.setVertexToShareInstancesWith(t3);
			i2.setVertexToShareInstancesWith(t2);
			t2.setVertexToShareInstancesWith(t3);
			t3.setVertexToShareInstancesWith(o1);

			// connect vertices
			i1.connectTo(t1);
			i2.connectTo(t2);
			t1.connectTo(t3);
			t2.connectTo(t3);
			t3.connectTo(o1);

			LibraryCacheManager.register(jobID, new String[0]);

			final ExecutionGraph eg = new ExecutionGraph(jg, INSTANCE_MANAGER);

			// test instance types in ExecutionGraph
			final InstanceRequestMap instanceRequestMap = new InstanceRequestMap();
			final ExecutionStage executionStage = eg.getCurrentExecutionStage();
			executionStage.collectRequiredInstanceTypes(instanceRequestMap, ExecutionState.CREATED);
			assertEquals(1, instanceRequestMap.size());
			assertEquals(2,
				(int) instanceRequestMap.getMaximumNumberOfInstances(INSTANCE_MANAGER.getDefaultInstanceType()));

			// stage0
			final ExecutionStage es = eg.getStage(0);
			ExecutionGroupVertex egv0 = null; // input1
			ExecutionGroupVertex egv1 = null; // input2
			ExecutionGroupVertex egv2 = null; // task1
			ExecutionGroupVertex egv3 = null; // task2
			ExecutionGroupVertex egv4 = null; // task3
			ExecutionGroupVertex egv5 = null; // output1
			if (es.getStageMember(0).getName().equals("Input 1")) {
				egv0 = es.getStageMember(0);
			} else if (es.getStageMember(0).getName().equals("Input 2")) {
				egv1 = es.getStageMember(0);
			} else if (es.getStageMember(0).getName().equals("Task 1")) {
				egv2 = es.getStageMember(0);
			} else if (es.getStageMember(0).getName().equals("Task 2")) {
				egv3 = es.getStageMember(0);
			} else if (es.getStageMember(0).getName().equals("Task 3")) {
				egv4 = es.getStageMember(0);
			} else {
				egv5 = es.getStageMember(0);
			}

			if (es.getStageMember(1).getName().equals("Input 1")) {
				egv0 = es.getStageMember(1);
			} else if (es.getStageMember(1).getName().equals("Input 2")) {
				egv1 = es.getStageMember(1);
			} else if (es.getStageMember(1).getName().equals("Task 1")) {
				egv2 = es.getStageMember(1);
			} else if (es.getStageMember(1).getName().equals("Task 2")) {
				egv3 = es.getStageMember(1);
			} else if (es.getStageMember(1).getName().equals("Task 3")) {
				egv4 = es.getStageMember(1);
			} else {
				egv5 = es.getStageMember(1);
			}
			if (es.getStageMember(2).getName().equals("Input 1")) {
				egv0 = es.getStageMember(2);
			} else if (es.getStageMember(2).getName().equals("Input 2")) {
				egv1 = es.getStageMember(2);
			} else if (es.getStageMember(2).getName().equals("Task 1")) {
				egv2 = es.getStageMember(2);
			} else if (es.getStageMember(2).getName().equals("Task 2")) {
				egv3 = es.getStageMember(2);
			} else if (es.getStageMember(2).getName().equals("Task 3")) {
				egv4 = es.getStageMember(2);
			} else {
				egv5 = es.getStageMember(2);
			}
			if (es.getStageMember(3).getName().equals("Input 1")) {
				egv0 = es.getStageMember(3);
			} else if (es.getStageMember(3).getName().equals("Input 2")) {
				egv1 = es.getStageMember(3);
			} else if (es.getStageMember(3).getName().equals("Task 1")) {
				egv2 = es.getStageMember(3);
			} else if (es.getStageMember(3).getName().equals("Task 2")) {
				egv3 = es.getStageMember(3);
			} else if (es.getStageMember(3).getName().equals("Task 3")) {
				egv4 = es.getStageMember(3);
			} else {
				egv5 = es.getStageMember(3);
			}
			if (es.getStageMember(4).getName().equals("Input 1")) {
				egv0 = es.getStageMember(4);
			} else if (es.getStageMember(4).getName().equals("Input 2")) {
				egv1 = es.getStageMember(4);
			} else if (es.getStageMember(4).getName().equals("Task 1")) {
				egv2 = es.getStageMember(4);
			} else if (es.getStageMember(4).getName().equals("Task 2")) {
				egv3 = es.getStageMember(4);
			} else if (es.getStageMember(4).getName().equals("Task 3")) {
				egv4 = es.getStageMember(4);
			} else {
				egv5 = es.getStageMember(4);
			}
			if (es.getStageMember(5).getName().equals("Input 1")) {
				egv0 = es.getStageMember(5);
			} else if (es.getStageMember(5).getName().equals("Input 2")) {
				egv1 = es.getStageMember(5);
			} else if (es.getStageMember(5).getName().equals("Task 1")) {
				egv2 = es.getStageMember(5);
			} else if (es.getStageMember(5).getName().equals("Task 2")) {
				egv3 = es.getStageMember(5);
			} else if (es.getStageMember(5).getName().equals("Task 3")) {
				egv4 = es.getStageMember(5);
			} else {
				egv5 = es.getStageMember(5);
			}
			final ExecutionVertex i1_0 = egv0.getGroupMember(0); // input1
			final ExecutionVertex i1_1 = egv0.getGroupMember(1); // input1
			final ExecutionVertex i2_0 = egv1.getGroupMember(0); // input2
			final ExecutionVertex i2_1 = egv1.getGroupMember(1); // input2
			final ExecutionVertex t1_0 = egv2.getGroupMember(0); // task1
			final ExecutionVertex t1_1 = egv2.getGroupMember(1); // task1
			final ExecutionVertex t2_0 = egv3.getGroupMember(0); // task2
			final ExecutionVertex t2_1 = egv3.getGroupMember(1); // task2
			final ExecutionVertex t3_0 = egv4.getGroupMember(0); // task3
			final ExecutionVertex t3_1 = egv4.getGroupMember(1); // task3
			final ExecutionVertex o1_0 = egv5.getGroupMember(0); // output1
			final ExecutionVertex o1_1 = egv5.getGroupMember(1); // otuput1

			// instance 1
			assertTrue((t1_0.getAllocatedResource().equals(i1_0.getAllocatedResource()) && !t1_0.getAllocatedResource()
				.equals(i1_1.getAllocatedResource()))
				|| (!t1_0.getAllocatedResource().equals(i1_0.getAllocatedResource()) && t1_0.getAllocatedResource()
					.equals(i1_1.getAllocatedResource())));
			assertTrue((t1_0.getAllocatedResource().equals(i2_0.getAllocatedResource()) && !t1_0.getAllocatedResource()
				.equals(i2_1.getAllocatedResource()))
				|| (!t1_0.getAllocatedResource().equals(i2_0.getAllocatedResource()) && t1_0.getAllocatedResource()
					.equals(i2_1.getAllocatedResource())));
			assertTrue((t1_0.getAllocatedResource().equals(t2_0.getAllocatedResource()) && !t1_0.getAllocatedResource()
				.equals(t2_1.getAllocatedResource()))
				|| (!t1_0.getAllocatedResource().equals(t2_0.getAllocatedResource()) && t1_0.getAllocatedResource()
					.equals(t2_1.getAllocatedResource())));
			assertTrue((t1_0.getAllocatedResource().equals(t3_0.getAllocatedResource()) && !t1_0.getAllocatedResource()
				.equals(t3_1.getAllocatedResource()))
				|| (!t1_0.getAllocatedResource().equals(t3_0.getAllocatedResource()) && t1_0.getAllocatedResource()
					.equals(t3_1.getAllocatedResource())));
			assertTrue((t1_0.getAllocatedResource().equals(o1_0.getAllocatedResource()) && !t1_0.getAllocatedResource()
				.equals(o1_1.getAllocatedResource()))
				|| (!t1_0.getAllocatedResource().equals(o1_0.getAllocatedResource()) && t1_0.getAllocatedResource()
					.equals(o1_1.getAllocatedResource())));
			// instance 2
			assertTrue((t1_1.getAllocatedResource().equals(i1_0.getAllocatedResource()) && !t1_1.getAllocatedResource()
				.equals(i1_1.getAllocatedResource()))
				|| (!t1_1.getAllocatedResource().equals(i1_0.getAllocatedResource()) && t1_1.getAllocatedResource()
					.equals(i1_1.getAllocatedResource())));
			assertTrue((t1_1.getAllocatedResource().equals(i2_0.getAllocatedResource()) && !t1_1.getAllocatedResource()
				.equals(i2_1.getAllocatedResource()))
				|| (!t1_1.getAllocatedResource().equals(i2_0.getAllocatedResource()) && t1_1.getAllocatedResource()
					.equals(i2_1.getAllocatedResource())));
			assertTrue((t1_1.getAllocatedResource().equals(t2_0.getAllocatedResource()) && !t1_1.getAllocatedResource()
				.equals(t2_1.getAllocatedResource()))
				|| (!t1_1.getAllocatedResource().equals(t2_0.getAllocatedResource()) && t1_1.getAllocatedResource()
					.equals(t2_1.getAllocatedResource())));
			assertTrue((t1_1.getAllocatedResource().equals(t3_0.getAllocatedResource()) && !t1_1.getAllocatedResource()
				.equals(t3_1.getAllocatedResource()))
				|| (!t1_1.getAllocatedResource().equals(t3_0.getAllocatedResource()) && t1_1.getAllocatedResource()
					.equals(t3_1.getAllocatedResource())));
			assertTrue((t1_1.getAllocatedResource().equals(o1_0.getAllocatedResource()) && !t1_1.getAllocatedResource()
				.equals(o1_1.getAllocatedResource()))
				|| (!t1_1.getAllocatedResource().equals(o1_0.getAllocatedResource()) && t1_1.getAllocatedResource()
					.equals(o1_1.getAllocatedResource())));
		} catch (GraphConversionException e) {
			fail(e.getMessage());
		} catch (IOException e) {
			fail(e.getMessage());
		} catch (JobGraphDefinitionException e) {
			fail(e.getMessage());
		} finally {
			if (inputFile1 != null) {
				inputFile1.delete();
			}
			if (inputFile2 != null) {
				inputFile2.delete();
			}
			if (outputFile != null) {
				outputFile.delete();
			}
			if (jobID != null) {
				try {
					LibraryCacheManager.unregister(jobID);
				} catch (IOException ioe) {
				}
			}
		}
	}

	/*
	 * input1 -> task1 -> output1
	 * -> task3 -> task4
	 * input2 -> task2 -> output2
	 * all subtasks defined
	 * all instance types defined
	 * all channel types defined
	 */
	@Test
	public void testConvertJobGraphToExecutionGraph4() {

		File inputFile1 = null;
		File inputFile2 = null;
		File outputFile1 = null;
		File outputFile2 = null;
		JobID jobID = null;

		try {

			inputFile1 = ServerTestUtils.createInputFile(0);
			inputFile2 = ServerTestUtils.createInputFile(0);
			outputFile1 = new File(ServerTestUtils.getRandomFilename());
			outputFile2 = new File(ServerTestUtils.getRandomFilename());

			// create job graph
			final JobGraph jg = new JobGraph("Job Graph 1");
			jobID = jg.getJobID();

			// input vertex
			final JobFileInputVertex i1 = new JobFileInputVertex("Input 1", jg);
			i1.setFileInputClass(FileLineReader.class);
			i1.setFilePath(new Path(inputFile1.toURI()));
			i1.setNumberOfSubtasks(4);
			i1.setNumberOfSubtasksPerInstance(2);
			final JobFileInputVertex i2 = new JobFileInputVertex("Input 2", jg);
			i2.setFileInputClass(FileLineReader.class);
			i2.setFilePath(new Path(inputFile2.toURI()));
			i2.setNumberOfSubtasks(4);
			i2.setNumberOfSubtasksPerInstance(2);
			// task vertex
			final JobTaskVertex t1 = new JobTaskVertex("Task 1", jg);
			t1.setTaskClass(ForwardTask1Input1Output.class);
			t1.setNumberOfSubtasks(4);
			t1.setNumberOfSubtasksPerInstance(2);
			final JobTaskVertex t2 = new JobTaskVertex("Task 2", jg);
			t2.setTaskClass(ForwardTask1Input1Output.class);
			t2.setNumberOfSubtasks(4);
			t2.setNumberOfSubtasksPerInstance(2);
			final JobTaskVertex t3 = new JobTaskVertex("Task 3", jg);
			t3.setTaskClass(ForwardTask2Inputs1Output.class);
			t3.setNumberOfSubtasks(8);
			t3.setNumberOfSubtasksPerInstance(4);
			final JobTaskVertex t4 = new JobTaskVertex("Task 4", jg);
			t4.setTaskClass(ForwardTask1Input2Outputs.class);
			t4.setNumberOfSubtasks(8);
			t4.setNumberOfSubtasksPerInstance(4);
			// output vertex
			final JobFileOutputVertex o1 = new JobFileOutputVertex("Output 1", jg);
			o1.setFileOutputClass(FileLineWriter.class);
			o1.setFilePath(new Path(outputFile1.toURI()));
			o1.setNumberOfSubtasks(4);
			o1.setNumberOfSubtasksPerInstance(2);
			final JobFileOutputVertex o2 = new JobFileOutputVertex("Output 2", jg);
			o2.setFileOutputClass(FileLineWriter.class);
			o2.setFilePath(new Path(outputFile2.toURI()));
			o2.setNumberOfSubtasks(4);
			o2.setNumberOfSubtasksPerInstance(2);
			o1.setVertexToShareInstancesWith(o2);

			// connect vertices
			i1.connectTo(t1, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);
			i2.connectTo(t2, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);
			t1.connectTo(t3, ChannelType.NETWORK);
			t2.connectTo(t3, ChannelType.NETWORK);
			t3.connectTo(t4, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);
			t4.connectTo(o1, ChannelType.NETWORK);
			t4.connectTo(o2, ChannelType.NETWORK);

			LibraryCacheManager.register(jobID, new String[0]);

			// now convert job graph to execution graph
			final ExecutionGraph eg = new ExecutionGraph(jg, INSTANCE_MANAGER);

			// test instance types in ExecutionGraph
			final InstanceRequestMap instanceRequestMap = new InstanceRequestMap();
			ExecutionStage executionStage = eg.getCurrentExecutionStage();
			assertNotNull(executionStage);
			assertEquals(0, executionStage.getStageNumber());
			
			executionStage.collectRequiredInstanceTypes(instanceRequestMap, ExecutionState.CREATED);
			assertEquals(1, instanceRequestMap.size());
			assertEquals(8,
				(int) instanceRequestMap.getMaximumNumberOfInstances(INSTANCE_MANAGER
					.getInstanceTypeByName(DEFAULT_INSTANCE_TYPE_NAME)));
			// Fake transition to next stage by triggering execution state changes manually
			final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(eg, eg.getIndexOfCurrentExecutionStage(),
				true, true);

			while (it.hasNext()) {
				final ExecutionVertex ev = it.next();
				ev.updateExecutionState(ExecutionState.SCHEDULED);
				ev.updateExecutionState(ExecutionState.ASSIGNED);
				ev.updateExecutionState(ExecutionState.READY);
				ev.updateExecutionState(ExecutionState.STARTING);
				ev.updateExecutionState(ExecutionState.RUNNING);
				ev.updateExecutionState(ExecutionState.FINISHING);
				ev.updateExecutionState(ExecutionState.FINISHED);
			}
			instanceRequestMap.clear();
		} catch (GraphConversionException e) {
			fail(e.getMessage());
		} catch (JobGraphDefinitionException e) {
			fail(e.getMessage());
		} catch (IOException e) {
			fail(e.getMessage());
		} finally {
			if (inputFile1 != null) {
				inputFile1.delete();
			}
			if (inputFile2 != null) {
				inputFile2.delete();
			}
			if (outputFile1 != null) {
				outputFile1.delete();
			}
			if (outputFile2 != null) {
				outputFile2.delete();
			}
			if (jobID != null) {
				try {
					LibraryCacheManager.unregister(jobID);
				} catch (IOException e) {
				}
			}
		}
	}

	/**
	 * Tests the conversion of a job graph representing a self cross to an execution graph.
	 */
	@Test
	public void testConvertSelfCross() {

		final String inputTaskName = "Self Cross Input";
		final String crossTaskName = "Self Cross Task";
		final String outputTaskName = "Self Cross Output";
		final int degreeOfParallelism = 4;
		File inputFile1 = null;
		File outputFile1 = null;
		JobID jobID = null;

		try {

			inputFile1 = ServerTestUtils.createInputFile(0);
			outputFile1 = new File(ServerTestUtils.getRandomFilename());

			// create job graph
			final JobGraph jg = new JobGraph("Self Cross Test Job");
			jobID = jg.getJobID();

			// input vertex
			final JobFileInputVertex input = new JobFileInputVertex(inputTaskName, jg);
			input.setFileInputClass(SelfCrossInputTask.class);
			input.setFilePath(new Path(inputFile1.toURI()));
			input.setNumberOfSubtasks(degreeOfParallelism);

			// cross vertex
			final JobTaskVertex cross = new JobTaskVertex(crossTaskName, jg);
			cross.setTaskClass(SelfCrossForwardTask.class);
			cross.setNumberOfSubtasks(degreeOfParallelism);

			// output vertex
			final JobFileOutputVertex output = new JobFileOutputVertex(outputTaskName, jg);
			output.setFileOutputClass(FileLineWriter.class);
			output.setFilePath(new Path(outputFile1.toURI()));
			output.setNumberOfSubtasks(degreeOfParallelism);

			// connect vertices
			input.connectTo(cross, ChannelType.IN_MEMORY, 0, 0,
				DistributionPattern.POINTWISE);
			input.connectTo(cross, ChannelType.NETWORK, 1, 1,
				DistributionPattern.BIPARTITE);
			cross.connectTo(output, ChannelType.IN_MEMORY, 0, 0,
				DistributionPattern.POINTWISE);

			LibraryCacheManager.register(jobID, new String[0]);

			// now convert job graph to execution graph
			final ExecutionGraph eg = new ExecutionGraph(jg, INSTANCE_MANAGER);

			assertEquals(1, eg.getNumberOfStages());

			final ExecutionStage stage = eg.getStage(0);

			assertEquals(3, stage.getNumberOfStageMembers());

			ExecutionGroupVertex inputGroupVertex = null;
			ExecutionGroupVertex crossGroupVertex = null;
			ExecutionGroupVertex outputGroupVertex = null;
			final ExecutionGroupVertexIterator groupIt = new ExecutionGroupVertexIterator(eg, true, -1);
			while (groupIt.hasNext()) {

				ExecutionGroupVertex gv = groupIt.next();
				if (inputTaskName.equals(gv.getName())) {
					inputGroupVertex = gv;
				} else if (crossTaskName.equals(gv.getName())) {
					crossGroupVertex = gv;
				} else if (outputTaskName.equals(gv.getName())) {
					outputGroupVertex = gv;
				}
			}

			assertNotNull(inputGroupVertex);
			assertNotNull(crossGroupVertex);
			assertNotNull(outputGroupVertex);

			assertEquals(degreeOfParallelism, inputGroupVertex.getCurrentNumberOfGroupMembers());
			assertEquals(degreeOfParallelism, crossGroupVertex.getCurrentNumberOfGroupMembers());
			assertEquals(degreeOfParallelism, outputGroupVertex.getCurrentNumberOfGroupMembers());

			// Check that all subtasks on a pipeline share the same instance
			assertEquals(inputGroupVertex.getGroupMember(0).getAllocatedResource(), crossGroupVertex.getGroupMember(0)
				.getAllocatedResource());
			assertEquals(inputGroupVertex.getGroupMember(1).getAllocatedResource(), crossGroupVertex.getGroupMember(1)
				.getAllocatedResource());
			assertEquals(inputGroupVertex.getGroupMember(2).getAllocatedResource(), crossGroupVertex.getGroupMember(2)
				.getAllocatedResource());
			assertEquals(inputGroupVertex.getGroupMember(3).getAllocatedResource(), crossGroupVertex.getGroupMember(3)
				.getAllocatedResource());

			assertEquals(crossGroupVertex.getGroupMember(0).getAllocatedResource(), outputGroupVertex.getGroupMember(0)
				.getAllocatedResource());
			assertEquals(crossGroupVertex.getGroupMember(1).getAllocatedResource(), outputGroupVertex.getGroupMember(1)
				.getAllocatedResource());
			assertEquals(crossGroupVertex.getGroupMember(2).getAllocatedResource(), outputGroupVertex.getGroupMember(2)
				.getAllocatedResource());
			assertEquals(crossGroupVertex.getGroupMember(3).getAllocatedResource(), outputGroupVertex.getGroupMember(3)
				.getAllocatedResource());

			// Check that all subtasks on different pipelines run on different instances
			assertFalse(inputGroupVertex.getGroupMember(0).getAllocatedResource()
				.equals(inputGroupVertex.getGroupMember(1).getAllocatedResource()));
			assertFalse(inputGroupVertex.getGroupMember(1).getAllocatedResource()
				.equals(inputGroupVertex.getGroupMember(2).getAllocatedResource()));
			assertFalse(inputGroupVertex.getGroupMember(2).getAllocatedResource()
				.equals(inputGroupVertex.getGroupMember(3).getAllocatedResource()));

		} catch (GraphConversionException e) {
			fail(e.getMessage());
		} catch (JobGraphDefinitionException e) {
			fail(e.getMessage());
		} catch (IOException ioe) {
			fail(ioe.getMessage());
		} finally {
			if (inputFile1 != null) {
				inputFile1.delete();
			}
			if (outputFile1 != null) {
				outputFile1.delete();
			}
			if (jobID != null) {
				try {
					LibraryCacheManager.unregister(jobID);
				} catch (IOException e) {
				}
			}
		}
	}

	/**
	 * This test checks the correctness of the instance sharing API. In particular, the test checks the behavior of the
	 * instance sharing as reported broken in ticket #198
	 */
	@Test
	public void testInstanceSharing() {

		final int degreeOfParallelism = 4;
		File inputFile1 = null;
		File outputFile1 = null;
		JobID jobID = null;

		try {

			inputFile1 = ServerTestUtils.createInputFile(0);
			outputFile1 = new File(ServerTestUtils.getRandomFilename());

			// create job graph
			final JobGraph jg = new JobGraph("Instance Sharing Test Job");
			jobID = jg.getJobID();

			// input vertex
			final JobFileInputVertex input1 = new JobFileInputVertex("Input 1", jg);
			input1.setFileInputClass(FileLineReader.class);
			input1.setFilePath(new Path(inputFile1.toURI()));
			input1.setNumberOfSubtasks(degreeOfParallelism);

			// forward vertex 1
			final JobTaskVertex forward1 = new JobTaskVertex("Forward 1", jg);
			forward1.setTaskClass(ForwardTask1Input1Output.class);
			forward1.setNumberOfSubtasks(degreeOfParallelism);

			// forward vertex 2
			final JobTaskVertex forward2 = new JobTaskVertex("Forward 2", jg);
			forward2.setTaskClass(ForwardTask1Input1Output.class);
			forward2.setNumberOfSubtasks(degreeOfParallelism);

			// forward vertex 3
			final JobTaskVertex forward3 = new JobTaskVertex("Forward 3", jg);
			forward3.setTaskClass(ForwardTask1Input1Output.class);
			forward3.setNumberOfSubtasks(degreeOfParallelism);

			// output vertex
			final JobFileOutputVertex output1 = new JobFileOutputVertex("Output 1", jg);
			output1.setFileOutputClass(FileLineWriter.class);
			output1.setFilePath(new Path(outputFile1.toURI()));
			output1.setNumberOfSubtasks(degreeOfParallelism);

			// connect vertices
			input1.connectTo(forward1, ChannelType.IN_MEMORY,
				DistributionPattern.POINTWISE);
			forward1.connectTo(forward2, ChannelType.IN_MEMORY,
				DistributionPattern.POINTWISE);
			forward2.connectTo(forward3, ChannelType.NETWORK,
				DistributionPattern.POINTWISE);
			forward3.connectTo(output1, ChannelType.IN_MEMORY);

			// setup instance sharing
			input1.setVertexToShareInstancesWith(forward1);
			forward1.setVertexToShareInstancesWith(forward2);
			forward2.setVertexToShareInstancesWith(forward3);
			forward3.setVertexToShareInstancesWith(output1);

			LibraryCacheManager.register(jobID, new String[0]);

			// now convert job graph to execution graph
			final ExecutionGraph eg = new ExecutionGraph(jg, INSTANCE_MANAGER);

			// Check number of stages
			assertEquals(1, eg.getNumberOfStages());

			// Check number of vertices in stage
			final ExecutionStage stage = eg.getStage(0);
			assertEquals(5, stage.getNumberOfStageMembers());

			// Check number of required instances
			final InstanceRequestMap instanceRequestMap = new InstanceRequestMap();
			stage.collectRequiredInstanceTypes(instanceRequestMap, ExecutionState.CREATED);

			// First, we expect all required instances to be of the same type
			assertEquals(1, instanceRequestMap.size());

			final int numberOfRequiredInstances = instanceRequestMap.getMinimumNumberOfInstances(INSTANCE_MANAGER
				.getDefaultInstanceType());
			assertEquals(degreeOfParallelism, numberOfRequiredInstances);

		} catch (GraphConversionException e) {
			fail(e.getMessage());
		} catch (JobGraphDefinitionException e) {
			fail(e.getMessage());
		} catch (IOException ioe) {
			fail(ioe.getMessage());
		} finally {
			if (inputFile1 != null) {
				inputFile1.delete();
			}
			if (outputFile1 != null) {
				outputFile1.delete();
			}
			if (jobID != null) {
				try {
					LibraryCacheManager.unregister(jobID);
				} catch (IOException e) {
				}
			}
		}
	}
}
