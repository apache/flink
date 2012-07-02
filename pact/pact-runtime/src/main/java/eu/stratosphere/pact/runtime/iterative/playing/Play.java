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
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
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
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.runtime.iterative.io.FakeOutputTask;
import eu.stratosphere.pact.runtime.iterative.task.*;
import eu.stratosphere.pact.runtime.task.DataSinkTask;
import eu.stratosphere.pact.runtime.task.DataSourceTask;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

import java.io.IOException;


public class Play {

  static final int MEGABYTE = 1024 * 1024;

  public static void main(String[] args) throws Exception {

    int degreeOfParallelism = 1;
    JobGraph jobGraph = new JobGraph();

    JobInputVertex input = new JobInputVertex("FileInput", jobGraph);
    Class<AbstractInputTask<?>> clazz = (Class<AbstractInputTask<?>>) (Class<?>) DataSourceTask.class;
    input.setInputClass(clazz);
    input.setNumberOfSubtasks(degreeOfParallelism);
    TaskConfig inputConfig = new TaskConfig(input.getConfiguration());
    inputConfig.setStubClass(TextInputFormat.class);
    inputConfig.setLocalStrategy(TaskConfig.LocalStrategy.NONE);
    inputConfig.setStubParameter(FileInputFormat.FILE_PARAMETER_KEY, "file:///home/ssc/Desktop/i.txt");

    JobTaskVertex head = createTask(BulkIterationHeadPactTask.class, "BulkIterationHead", jobGraph, degreeOfParallelism);
    TaskConfig headConfig = new TaskConfig(head.getConfiguration());
    headConfig.setDriver(MapDriver.class);
    headConfig.setStubClass(AppendMapper.AppendHeadMapper.class);
    headConfig.setMemorySize(10 * MEGABYTE);

    JobTaskVertex intermediate = createTask(BulkIterationIntermediatePactTask.class, "BulkIntermediate", jobGraph,
        degreeOfParallelism);
    TaskConfig intermediateConfig = new TaskConfig(intermediate.getConfiguration());
    intermediateConfig.setDriver(MapDriver.class);
    intermediateConfig.setStubClass(AppendMapper.AppendIntermediateMapper.class);

    JobTaskVertex tail = createTask(BulkIterationTailPactTask.class, "BulkIterationTail", jobGraph, degreeOfParallelism);
    TaskConfig tailConfig = new TaskConfig(tail.getConfiguration());
    tailConfig.setDriver(MapDriver.class);
    tailConfig.setStubClass(AppendMapper.AppendTailMapper.class);

    JobTaskVertex sync = createTask(BulkIterationSynchronizationPactTask.class, "BulkIterationSynch", jobGraph,
        degreeOfParallelism);
    TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
    syncConfig.setDriver(MapDriver.class);
    syncConfig.setStubClass(EmptyMapStub.class);

    JobOutputVertex output = createFileOutput(jobGraph, "FinalOutput", degreeOfParallelism);
    TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
    outputConfig.setStubClass(PlayOutFormat.class);
    outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, "file:///tmp/stratosphere/iterations");


    JobOutputVertex tailBlindOutput = createFakeOutput(jobGraph, "FakeTailOutput", degreeOfParallelism);
    JobOutputVertex syncBlindOutput = createFakeOutput(jobGraph, "FakeSyncOutput", degreeOfParallelism);

    connectLocal(input, head, inputConfig);
    connectLocal(head, intermediate, headConfig);
    connectLocal(head, sync, headConfig);
    connectLocal(head, output, headConfig);
    connectLocal(intermediate, tail, intermediateConfig);
    connectLocal(tail, tailBlindOutput, tailConfig);
    connectLocal(sync, syncBlindOutput, syncConfig);

    head.setVertexToShareInstancesWith(tail);

    GlobalConfiguration.loadConfiguration(
        "/home/ssc/Entwicklung/projects/stratosphere-iterations/stratosphere-dist/src/main/stratosphere-bin/conf");
    Configuration conf = GlobalConfiguration.getConfiguration();

    submit(jobGraph, conf);
  }

  static void submit(JobGraph graph, Configuration nepheleConfig) throws IOException, JobExecutionException {
    JobClient client = new JobClient(graph, nepheleConfig);
    client.submitJobAndWait();
  }

  static void connectLocal(AbstractJobVertex source, AbstractJobVertex target, TaskConfig sourceConfig)
      throws JobGraphDefinitionException {
    source.connectTo(target, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
    sourceConfig.addOutputShipStrategy(OutputEmitter.ShipStrategy.FORWARD);
  }

  static JobTaskVertex createTask(Class<? extends RegularPactTask> task, String name, JobGraph graph, int dop) {
    JobTaskVertex taskVertex = new JobTaskVertex(name, graph);
    taskVertex.setTaskClass(task);
    taskVertex.setNumberOfSubtasks(dop);
    return taskVertex;
  }

  static JobOutputVertex createFakeOutput(JobGraph jobGraph, String name, int degreeOfParallelism) {
    JobOutputVertex outputVertex = new JobOutputVertex(name, jobGraph);
    outputVertex.setOutputClass(FakeOutputTask.class);
    outputVertex.setNumberOfSubtasks(degreeOfParallelism);
    return outputVertex;
  }

  static JobOutputVertex createFileOutput(JobGraph jobGraph, String name, int degreeOfParallelism) {
    JobOutputVertex sinkVertex = new JobOutputVertex(name, jobGraph);
    sinkVertex.setOutputClass(DataSinkTask.class);
    sinkVertex.setNumberOfSubtasks(degreeOfParallelism);
    return sinkVertex;
  }
}

