/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.test.iterative.nephele;

import java.io.IOException;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.generic.contract.UserCodeClassWrapper;
import eu.stratosphere.pact.generic.contract.UserCodeWrapper;
import eu.stratosphere.pact.generic.io.InputFormat;
import eu.stratosphere.pact.runtime.iterative.io.FakeOutputTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationSynchronizationSinkTask;
import eu.stratosphere.pact.runtime.task.DataSinkTask;
import eu.stratosphere.pact.runtime.task.DataSourceTask;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class JobGraphUtils {

	public static final long MEGABYTE = 1024l * 1024l;

	private JobGraphUtils() {
	}

	public static void submit(JobGraph graph, Configuration nepheleConfig) throws IOException, JobExecutionException {
		JobClient client = new JobClient(graph, nepheleConfig);
		client.submitJobAndWait();
	}
	
	public static <T extends InputFormat<?,?>> JobInputVertex createInput(Class<? extends T> stub, String path, String name, JobGraph graph,
			int degreeOfParallelism, int numSubTasksPerInstance) {
		return createInput(new UserCodeClassWrapper<T>(stub), path, name, graph, degreeOfParallelism, numSubTasksPerInstance);
	}

	public static <T extends InputFormat<?,?>> JobInputVertex createInput(UserCodeWrapper<T> stub, String path, String name, JobGraph graph,
			int degreeOfParallelism, int numSubTasksPerInstance)
	{
		JobInputVertex inputVertex = new JobInputVertex(name, graph);
		@SuppressWarnings("unchecked")
		Class<AbstractInputTask<?>> clazz = (Class<AbstractInputTask<?>>) (Class<?>) DataSourceTask.class;
		inputVertex.setInputClass(clazz);
		inputVertex.setNumberOfSubtasks(degreeOfParallelism);
		inputVertex.setNumberOfSubtasksPerInstance(numSubTasksPerInstance);
		TaskConfig inputConfig = new TaskConfig(inputVertex.getConfiguration());
		inputConfig.setStubWrapper(stub);
		inputConfig.setStubParameter(FileInputFormat.FILE_PARAMETER_KEY, path);
		return inputVertex;
	}

//	public static void connect(AbstractJobVertex source, AbstractJobVertex target, ChannelType channelType,
//			DistributionPattern distributionPattern, ShipStrategyType shipStrategy) throws JobGraphDefinitionException
//	{
//		source.connectTo(target, channelType, CompressionLevel.NO_COMPRESSION, distributionPattern);
//		new TaskConfig(source.getConfiguration()).addOutputShipStrategy(shipStrategy);
//	}
	
	public static void connect(AbstractJobVertex source, AbstractJobVertex target, ChannelType channelType,
			DistributionPattern distributionPattern) throws JobGraphDefinitionException
	{
		source.connectTo(target, channelType, distributionPattern);
	}

	public static JobTaskVertex createTask(@SuppressWarnings("rawtypes") Class<? extends RegularPactTask> task, String name, JobGraph graph,
			int degreeOfParallelism, int numSubtasksPerInstance)
	{
		JobTaskVertex taskVertex = new JobTaskVertex(name, graph);
		taskVertex.setTaskClass(task);
		taskVertex.setNumberOfSubtasks(degreeOfParallelism);
		taskVertex.setNumberOfSubtasksPerInstance(numSubtasksPerInstance);
		return taskVertex;
	}

	public static JobOutputVertex createSync(JobGraph jobGraph, int degreeOfParallelism) {
		JobOutputVertex sync = new JobOutputVertex("BulkIterationSync", jobGraph);
		sync.setOutputClass(IterationSynchronizationSinkTask.class);
		sync.setNumberOfSubtasks(1);
		TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
		syncConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, degreeOfParallelism);
		return sync;
	}

	public static JobOutputVertex createFakeOutput(JobGraph jobGraph, String name, int degreeOfParallelism,
			int numSubTasksPerInstance)
	{
		JobOutputVertex outputVertex = new JobOutputVertex(name, jobGraph);
		outputVertex.setOutputClass(FakeOutputTask.class);
		outputVertex.setNumberOfSubtasks(degreeOfParallelism);
		outputVertex.setNumberOfSubtasksPerInstance(numSubTasksPerInstance);
		return outputVertex;
	}

	public static JobOutputVertex createFileOutput(JobGraph jobGraph, String name, int degreeOfParallelism,
			int numSubTasksPerInstance)
	{
		JobOutputVertex sinkVertex = new JobOutputVertex(name, jobGraph);
		sinkVertex.setOutputClass(DataSinkTask.class);
		sinkVertex.setNumberOfSubtasks(degreeOfParallelism);
		sinkVertex.setNumberOfSubtasksPerInstance(numSubTasksPerInstance);
		return sinkVertex;
	}
}
