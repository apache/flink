/**
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


package org.apache.flink.test.iterative.nephele;

import java.io.IOException;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.io.network.channels.ChannelType;
import org.apache.flink.runtime.iterative.io.FakeOutputTask;
import org.apache.flink.runtime.iterative.task.IterationSynchronizationSinkTask;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphDefinitionException;
import org.apache.flink.runtime.jobgraph.JobInputVertex;
import org.apache.flink.runtime.jobgraph.JobOutputVertex;
import org.apache.flink.runtime.jobgraph.JobTaskVertex;
import org.apache.flink.runtime.operators.DataSinkTask;
import org.apache.flink.runtime.operators.DataSourceTask;
import org.apache.flink.runtime.operators.RegularPactTask;
import org.apache.flink.runtime.operators.util.TaskConfig;

public class JobGraphUtils {

	public static final long MEGABYTE = 1024l * 1024l;

	private JobGraphUtils() {
	}

	public static void submit(JobGraph graph, Configuration nepheleConfig) throws IOException, JobExecutionException {
		JobClient client = new JobClient(graph, nepheleConfig, JobGraphUtils.class.getClassLoader());
		client.submitJobAndWait();
	}
	
	public static <T extends FileInputFormat<?>> JobInputVertex createInput(T stub, String path, String name, JobGraph graph,
			int degreeOfParallelism)
	{
		stub.setFilePath(path);
		return createInput(new UserCodeObjectWrapper<T>(stub), name, graph, degreeOfParallelism);
	}

	private static <T extends InputFormat<?,?>> JobInputVertex createInput(UserCodeWrapper<T> stub, String name, JobGraph graph,
			int degreeOfParallelism)
	{
		JobInputVertex inputVertex = new JobInputVertex(name, graph);
		
		inputVertex.setInvokableClass(DataSourceTask.class);
		
		inputVertex.setNumberOfSubtasks(degreeOfParallelism);

		TaskConfig inputConfig = new TaskConfig(inputVertex.getConfiguration());
		inputConfig.setStubWrapper(stub);
		
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
			int degreeOfParallelism)
	{
		JobTaskVertex taskVertex = new JobTaskVertex(name, graph);
		taskVertex.setInvokableClass(task);
		taskVertex.setNumberOfSubtasks(degreeOfParallelism);
		return taskVertex;
	}

	public static JobOutputVertex createSync(JobGraph jobGraph, int degreeOfParallelism) {
		JobOutputVertex sync = new JobOutputVertex("BulkIterationSync", jobGraph);
		sync.setInvokableClass(IterationSynchronizationSinkTask.class);
		sync.setNumberOfSubtasks(1);
		TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
		syncConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, degreeOfParallelism);
		return sync;
	}

	public static JobOutputVertex createFakeOutput(JobGraph jobGraph, String name, int degreeOfParallelism)
	{
		JobOutputVertex outputVertex = new JobOutputVertex(name, jobGraph);
		outputVertex.setInvokableClass(FakeOutputTask.class);
		outputVertex.setNumberOfSubtasks(degreeOfParallelism);
		return outputVertex;
	}

	public static JobOutputVertex createFileOutput(JobGraph jobGraph, String name, int degreeOfParallelism)
	{
		JobOutputVertex sinkVertex = new JobOutputVertex(name, jobGraph);
		sinkVertex.setInvokableClass(DataSinkTask.class);
		sinkVertex.setNumberOfSubtasks(degreeOfParallelism);
		return sinkVertex;
	}
}
