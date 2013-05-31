/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.jobmanager.scheduler.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.GraphConversionException;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobmanager.scheduler.SchedulingException;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * @author marrus
 *         This class checks the functionality of the {@link QueueScheduler} class
 */
public class QueueSchedulerTest {

	/**
	 * Test input task.
	 * 
	 * @author warneke
	 */
	public static final class InputTask extends AbstractGenericInputTask {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {
			new RecordWriter<StringRecord>(this, StringRecord.class);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {
			// Nothing to do here
		}

	}

	/**
	 * Test output task.
	 * 
	 * @author warneke
	 */
	public static final class OutputTask extends AbstractOutputTask {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {
			new RecordReader<StringRecord>(this, StringRecord.class);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {
			// Nothing to do here
		}

	}

	/**
	 * Constructs a sample execution graph consisting of two vertices connected by a channel of the given type.
	 * 
	 * @param channelType
	 *        the channel type to connect the vertices with
	 * @param instanceManager
	 *        the instance manager that shall be used during the creation of the execution graph
	 * @return a sample execution graph
	 */
	private ExecutionGraph createExecutionGraph(final ChannelType channelType, final InstanceManager instanceManager) {

		final JobGraph jobGraph = new JobGraph("Job Graph");

		final JobInputVertex inputVertex = new JobInputVertex("Input 1", jobGraph);
		inputVertex.setInputClass(InputTask.class);
		inputVertex.setNumberOfSubtasks(1);

		final JobOutputVertex outputVertex = new JobOutputVertex("Output 1", jobGraph);
		outputVertex.setOutputClass(OutputTask.class);
		outputVertex.setNumberOfSubtasks(1);

		try {
			inputVertex.connectTo(outputVertex, channelType);
		} catch (JobGraphDefinitionException e) {
			fail(StringUtils.stringifyException(e));
		}

		try {
			LibraryCacheManager.register(jobGraph.getJobID(), new String[0]);
			return new ExecutionGraph(jobGraph, instanceManager);

		} catch (GraphConversionException e) {
			fail(StringUtils.stringifyException(e));
		} catch (IOException e) {
			fail(StringUtils.stringifyException(e));
		}

		return null;
	}

	/**
	 * Checks the behavior of the scheduleJob() method with a job consisting of two tasks connected via an in-memory
	 * channel.
	 */
	@Test
	public void testSchedulJobWithInMemoryChannel() {

		final TestInstanceManager tim = new TestInstanceManager();
		final TestDeploymentManager tdm = new TestDeploymentManager();
		final QueueScheduler scheduler = new QueueScheduler(tdm, tim);

		final ExecutionGraph executionGraph = createExecutionGraph(ChannelType.INMEMORY, tim);

		try {
			try {
				scheduler.schedulJob(executionGraph);
			} catch (SchedulingException e) {
				fail(StringUtils.stringifyException(e));
			}

			// Wait for the deployment to complete
			tdm.waitForDeployment();

			assertEquals(executionGraph.getJobID(), tdm.getIDOfLastDeployedJob());
			final List<ExecutionVertex> listOfDeployedVertices = tdm.getListOfLastDeployedVertices();
			assertNotNull(listOfDeployedVertices);
			// Vertices connected via in-memory channels must be deployed in a single cycle.
			assertEquals(2, listOfDeployedVertices.size());

			// Check if the release of the allocated resources works properly by simulating the vertices' life cycle
			assertEquals(0, tim.getNumberOfReleaseMethodCalls());

			// Simulate vertex life cycle
			for (final ExecutionVertex vertex : listOfDeployedVertices) {
				vertex.updateExecutionState(ExecutionState.STARTING);
				vertex.updateExecutionState(ExecutionState.RUNNING);
				vertex.updateExecutionState(ExecutionState.FINISHING);
				vertex.updateExecutionState(ExecutionState.FINISHED);
			}

			assertEquals(1, tim.getNumberOfReleaseMethodCalls());
		} finally {
			try {
				LibraryCacheManager.unregister(executionGraph.getJobID());
			} catch (IOException ioe) {
				// Ignore exception here
			}
		}
	}
}
