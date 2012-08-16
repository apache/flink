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

package eu.stratosphere.pact.runtime.iterative.playing;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.runtime.iterative.io.FakeOutputTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationSynchronizationSinkTask;
import eu.stratosphere.pact.runtime.task.DataSinkTask;
import eu.stratosphere.pact.runtime.task.DataSourceTask;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

import java.io.IOException;

public class JobGraphUtils {

  public static final int MEGABYTE = 1024 * 1024;

  private JobGraphUtils() {}

  public static void submit(JobGraph graph, Configuration nepheleConfig) throws IOException, JobExecutionException {
    JobClient client = new JobClient(graph, nepheleConfig);
    client.submitJobAndWait();
  }

  public static JobInputVertex createInput(Class<?> stubClass, String path, String name, JobGraph graph,
      int degreeOfParallelism) {
    JobInputVertex inputVertex = new JobInputVertex(name, graph);
    Class<AbstractInputTask<?>> clazz = (Class<AbstractInputTask<?>>) (Class<?>) DataSourceTask.class;
    inputVertex.setInputClass(clazz);
    inputVertex.setNumberOfSubtasks(degreeOfParallelism);
    inputVertex.setNumberOfSubtasksPerInstance(degreeOfParallelism);
    TaskConfig inputConfig = new TaskConfig(inputVertex.getConfiguration());
    inputConfig.setStubClass(stubClass);
    inputConfig.setStubParameter(FileInputFormat.FILE_PARAMETER_KEY, path);
    return inputVertex;
  }

  public static void connect(AbstractJobVertex source, AbstractJobVertex target, ChannelType channelType,
      DistributionPattern distributionPattern, ShipStrategy shipStrategy) throws JobGraphDefinitionException {
    source.connectTo(target, channelType, CompressionLevel.NO_COMPRESSION, distributionPattern);
    new TaskConfig(source.getConfiguration()).addOutputShipStrategy(shipStrategy);
  }

  public static JobTaskVertex createTask(Class<? extends RegularPactTask> task, String name, JobGraph graph, int dop) {
    JobTaskVertex taskVertex = new JobTaskVertex(name, graph);
    taskVertex.setTaskClass(task);
    taskVertex.setNumberOfSubtasks(dop);
    taskVertex.setNumberOfSubtasksPerInstance(dop);
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

  public static JobOutputVertex createFakeOutput(JobGraph jobGraph, String name, int degreeOfParallelism) {
    JobOutputVertex outputVertex = new JobOutputVertex(name, jobGraph);
    outputVertex.setOutputClass(FakeOutputTask.class);
    outputVertex.setNumberOfSubtasks(degreeOfParallelism);
    outputVertex.setNumberOfSubtasksPerInstance(degreeOfParallelism);
    return outputVertex;
  }

  public static JobOutputVertex createFileOutput(JobGraph jobGraph, String name, int degreeOfParallelism) {
    JobOutputVertex sinkVertex = new JobOutputVertex(name, jobGraph);
    sinkVertex.setOutputClass(DataSinkTask.class);
    sinkVertex.setNumberOfSubtasks(degreeOfParallelism);
    sinkVertex.setNumberOfSubtasksPerInstance(degreeOfParallelism);
    return sinkVertex;
  }
}
