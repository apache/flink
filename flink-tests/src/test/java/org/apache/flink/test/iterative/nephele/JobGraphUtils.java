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

package org.apache.flink.test.iterative.nephele;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.runtime.iterative.task.IterationSynchronizationSinkTask;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputFormatVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.OutputFormatVertex;
import org.apache.flink.runtime.operators.DataSinkTask;
import org.apache.flink.runtime.operators.DataSourceTask;
import org.apache.flink.runtime.operators.RegularPactTask;
import org.apache.flink.runtime.operators.util.TaskConfig;

public class JobGraphUtils {

	public static final long MEGABYTE = 1024l * 1024l;

	private JobGraphUtils() {}
	
	public static <T extends FileInputFormat<?>> InputFormatVertex createInput(T stub, String path, String name, JobGraph graph,
			int parallelism)
	{
		stub.setFilePath(path);
		return createInput(new UserCodeObjectWrapper<T>(stub), name, graph, parallelism);
	}

	private static <T extends InputFormat<?,?>> InputFormatVertex createInput(UserCodeWrapper<T> stub, String name, JobGraph graph,
			int parallelism)
	{
		InputFormatVertex inputVertex = new InputFormatVertex(name);
		graph.addVertex(inputVertex);
		
		inputVertex.setInvokableClass(DataSourceTask.class);
		inputVertex.setParallelism(parallelism);

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
	
	public static void connect(AbstractJobVertex source, AbstractJobVertex target, DistributionPattern distributionPattern) {
		target.connectNewDataSetAsInput(source, distributionPattern);
	}

	@SuppressWarnings("rawtypes") 
	public static AbstractJobVertex createTask(Class<? extends RegularPactTask> task, String name, JobGraph graph, int parallelism)
	{
		AbstractJobVertex taskVertex = new AbstractJobVertex(name);
		graph.addVertex(taskVertex);
		
		taskVertex.setInvokableClass(task);
		taskVertex.setParallelism(parallelism);
		return taskVertex;
	}

	public static AbstractJobVertex createSync(JobGraph jobGraph, int parallelism) {
		AbstractJobVertex sync = new AbstractJobVertex("BulkIterationSync");
		jobGraph.addVertex(sync);
		
		sync.setInvokableClass(IterationSynchronizationSinkTask.class);
		sync.setParallelism(1);
		
		TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
		syncConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, parallelism);
		return sync;
	}

	public static OutputFormatVertex createFileOutput(JobGraph jobGraph, String name, int parallelism) {
		OutputFormatVertex sinkVertex = new OutputFormatVertex(name);
		jobGraph.addVertex(sinkVertex);
		
		sinkVertex.setInvokableClass(DataSinkTask.class);
		sinkVertex.setParallelism(parallelism);
		return sinkVertex;
	}
}
