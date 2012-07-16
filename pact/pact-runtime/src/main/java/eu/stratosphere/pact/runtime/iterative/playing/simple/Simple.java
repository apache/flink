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

package eu.stratosphere.pact.runtime.iterative.playing.simple;


import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.runtime.iterative.playing.JobGraphUtils;
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationIntermediatePactTask;
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationSynchronizationPactTask;
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationTailPactTask;
import eu.stratosphere.pact.runtime.iterative.task.EmptyMapStub;
import eu.stratosphere.pact.runtime.task.DataSourceTask;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class Simple {

  public static void main(String[] args) throws Exception {

    int degreeOfParallelism = 2;
    JobGraph jobGraph = new JobGraph();

    JobInputVertex input = new JobInputVertex("FileInput", jobGraph);
    Class<AbstractInputTask<?>> clazz = (Class<AbstractInputTask<?>>) (Class<?>) DataSourceTask.class;
    input.setInputClass(clazz);
    input.setNumberOfSubtasks(degreeOfParallelism);
    input.setNumberOfSubtasksPerInstance(degreeOfParallelism);
    TaskConfig inputConfig = new TaskConfig(input.getConfiguration());
    inputConfig.setStubClass(TextInputFormat.class);
    inputConfig.setLocalStrategy(TaskConfig.LocalStrategy.NONE);
    inputConfig.setStubParameter(FileInputFormat.FILE_PARAMETER_KEY, "file:///home/ssc/Desktop/iterations/");

    JobTaskVertex head = JobGraphUtils.createTask(BulkIterationHeadPactTask.class, "BulkIterationHead", jobGraph,
        degreeOfParallelism);
    TaskConfig headConfig = new TaskConfig(head.getConfiguration());
    headConfig.setDriver(MapDriver.class);
    headConfig.setStubClass(AppendMapper.AppendHeadMapper.class);
    headConfig.setMemorySize(10 * JobGraphUtils.MEGABYTE);
    headConfig.setBackChannelMemoryFraction(0.8f);
    headConfig.setNumberOfIterations(3);

    JobTaskVertex intermediate = JobGraphUtils.createTask(BulkIterationIntermediatePactTask.class, "BulkIntermediate",
        jobGraph, degreeOfParallelism);
    TaskConfig intermediateConfig = new TaskConfig(intermediate.getConfiguration());
    intermediateConfig.setDriver(MapDriver.class);
    intermediateConfig.setStubClass(AppendMapper.AppendIntermediateMapper.class);
    intermediateConfig.setNumberOfEventsUntilInterruptInIterativeGate(0, 1);

    JobTaskVertex tail = JobGraphUtils.createTask(BulkIterationTailPactTask.class, "BulkIterationTail", jobGraph,
        degreeOfParallelism);
    TaskConfig tailConfig = new TaskConfig(tail.getConfiguration());
    tailConfig.setDriver(MapDriver.class);
    tailConfig.setStubClass(AppendMapper.AppendTailMapper.class);
    tailConfig.setNumberOfEventsUntilInterruptInIterativeGate(0, 1);

    JobTaskVertex sync = JobGraphUtils.createSingletonTask(BulkIterationSynchronizationPactTask.class, "BulkIterationSync",
        jobGraph);
    TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
    syncConfig.setDriver(MapDriver.class);
    syncConfig.setStubClass(EmptyMapStub.class);
    syncConfig.setNumberOfEventsUntilInterruptInIterativeGate(0, degreeOfParallelism);

    JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "FinalOutput", degreeOfParallelism);
    TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
    outputConfig.setStubClass(SimpleOutFormat.class);
    outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, "file:///tmp/stratosphere/iterations");


    JobOutputVertex fakeTailOutput = JobGraphUtils.createFakeOutput(jobGraph, "FakeTailOutput", degreeOfParallelism);
    JobOutputVertex fakeSyncOutput = JobGraphUtils.createSingletonFakeOutput(jobGraph, "FakeSyncOutput");

    JobGraphUtils.connectLocal(input, head, inputConfig);
    //TODO implicit order should be documented/configured somehow
    JobGraphUtils.connectLocal(head, intermediate, headConfig);
    JobGraphUtils.connectLocal(head, sync, headConfig);
    JobGraphUtils.connectLocal(head, output, headConfig);
    JobGraphUtils.connectLocal(intermediate, tail, intermediateConfig);
    JobGraphUtils.connectLocal(tail, fakeTailOutput, tailConfig);
    JobGraphUtils.connectLocal(sync, fakeSyncOutput, syncConfig);

    head.setVertexToShareInstancesWith(tail);

    GlobalConfiguration.loadConfiguration(
        "/home/ssc/Entwicklung/projects/stratosphere-iterations/stratosphere-dist/src/main/stratosphere-bin/conf");
    Configuration conf = GlobalConfiguration.getConfiguration();

    JobGraphUtils.submit(jobGraph, conf);
  }


}

