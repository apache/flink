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

package eu.stratosphere.nephele.jobmanager.scheduler.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import eu.stratosphere.nephele.jobmanager.scheduler.DefaultScheduler;

import org.junit.Test;

import eu.stratosphere.api.common.io.GenericInputFormat;
import eu.stratosphere.api.common.io.OutputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.GraphConversionException;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobmanager.scheduler.SchedulingException;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.runtime.io.api.RecordReader;
import eu.stratosphere.runtime.io.api.RecordWriter;
import eu.stratosphere.runtime.io.channels.ChannelType;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.util.StringUtils;

/**
 * This class checks the functionality of the {@link eu.stratosphere.nephele.jobmanager.scheduler.DefaultScheduler} class
 */
@SuppressWarnings("serial")
public class DefaultSchedulerTest {


	public static final class InputTask extends AbstractInvokable {

		@Override
		public void registerInputOutput() {
			new RecordWriter<StringRecord>(this);
		}

		@Override
		public void invoke() throws Exception {}

	}

	public static final class OutputTask extends AbstractInvokable {

		@Override
		public void registerInputOutput() {
			new RecordReader<StringRecord>(this, StringRecord.class);
		}

		@Override
		public void invoke() throws Exception {}

	}

	public static final class DummyInputFormat extends GenericInputFormat<IntValue> {

		@Override
		public boolean reachedEnd() throws IOException {
			return true;
		}

		@Override
		public IntValue nextRecord(IntValue reuse) throws IOException {
			return null;
		}
	}

	public static final class DummyOutputFormat implements OutputFormat<IntValue> {

		@Override
		public void configure(Configuration parameters) {}

		@Override
		public void open(int taskNumber, int numTasks) {}

		@Override
		public void writeRecord(IntValue record) {}

		@Override
		public void close() {}
	}

	/**
	 * Constructs a sample execution graph consisting of two vertices connected by a channel of the given type.
	 * 
	 * @param channelType
	 *        the channel type to connect the vertices with
	 * @return a sample execution graph
	 */
	private ExecutionGraph createExecutionGraph(ChannelType channelType) {

		final JobGraph jobGraph = new JobGraph("Job Graph");

		final JobInputVertex inputVertex = new JobInputVertex("Input 1", jobGraph);
		inputVertex.setInvokableClass(InputTask.class);
		inputVertex.setInputFormat(new DummyInputFormat());
		inputVertex.setNumberOfSubtasks(1);

		final JobOutputVertex outputVertex = new JobOutputVertex("Output 1", jobGraph);
		outputVertex.setInvokableClass(OutputTask.class);
		outputVertex.setOutputFormat(new DummyOutputFormat());
		outputVertex.setNumberOfSubtasks(1);

		try {
			inputVertex.connectTo(outputVertex, channelType);
		} catch (JobGraphDefinitionException e) {
			fail(StringUtils.stringifyException(e));
		}

		try {
			LibraryCacheManager.register(jobGraph.getJobID(), new String[0]);
			return new ExecutionGraph(jobGraph, 1);

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
	public void testScheduleJobWithInMemoryChannel() {

		final TestInstanceManager tim = new TestInstanceManager();
		final TestDeploymentManager tdm = new TestDeploymentManager();
		final DefaultScheduler scheduler = new DefaultScheduler(tdm, tim);

		final ExecutionGraph executionGraph = createExecutionGraph(ChannelType.IN_MEMORY);

		try {
			try {
				scheduler.scheduleJob(executionGraph);
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
