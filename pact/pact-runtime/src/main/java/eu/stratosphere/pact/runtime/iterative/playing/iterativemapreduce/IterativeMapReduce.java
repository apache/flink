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

package eu.stratosphere.pact.runtime.iterative.playing.iterativemapreduce;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.runtime.iterative.playing.JobGraphUtils;
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationSynchronizationPactTask;
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationTailPactTask;
import eu.stratosphere.pact.runtime.iterative.task.EmptyMapStub;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.ReduceDriver;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class IterativeMapReduce {

  public static void main(String[] args) throws Exception {

    int degreeOfParallelism = 2;
    JobGraph jobGraph = new JobGraph();

    JobInputVertex input = JobGraphUtils.createInput(TokenTokenInputFormat.class,
        "file:///home/ssc/Desktop/stratosphere/test-inputs/iterative-mapreduce/", "FileInput", jobGraph,
        degreeOfParallelism);

    JobTaskVertex head = JobGraphUtils.createTask(BulkIterationHeadPactTask.class, "BulkIterationHead", jobGraph,
        degreeOfParallelism);
    TaskConfig headConfig = new TaskConfig(head.getConfiguration());
    headConfig.setDriver(MapDriver.class);
    headConfig.setStubClass(AppendTokenMapper.class);
    headConfig.setMemorySize(10 * JobGraphUtils.MEGABYTE);
    headConfig.setBackChannelMemoryFraction(0.8f);
    headConfig.setNumberOfIterations(3);
    headConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(head.getConfiguration(),
        headConfig.getPrefixForOutputParameters(0), new int[] { 0 }, new Class[] { PactString.class });

    JobTaskVertex tail = JobGraphUtils.createTask(BulkIterationTailPactTask.class, "BulkIterationTail", jobGraph,
        degreeOfParallelism);
    TaskConfig tailConfig = new TaskConfig(tail.getConfiguration());
    tailConfig.setLocalStrategy(TaskConfig.LocalStrategy.SORT);
    tailConfig.setDriver(ReduceDriver.class);
    tailConfig.setStubClass(AppendTokenReducer.class);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(tail.getConfiguration(),
        tailConfig.getPrefixForInputParameters(0), new int[] { 0 }, new Class[] { PactString.class });
    tailConfig.setMemorySize(3 * JobGraphUtils.MEGABYTE);
    tailConfig.setNumFilehandles(2);
    tailConfig.setGateIterativeAndSetNumberOfEventsUntilInterrupt(0, degreeOfParallelism);

    JobTaskVertex sync = JobGraphUtils.createSingletonTask(BulkIterationSynchronizationPactTask.class, "BulkIterationSync",
        jobGraph);
    TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
    syncConfig.setDriver(MapDriver.class);
    syncConfig.setStubClass(EmptyMapStub.class);
    syncConfig.setGateIterativeAndSetNumberOfEventsUntilInterrupt(0, degreeOfParallelism);

    JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "FinalOutput", degreeOfParallelism);
    TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
    outputConfig.setStubClass(AppendTokenOutFormat.class);
    outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, "file:///tmp/stratosphere/iterations");

    JobOutputVertex fakeTailOutput = JobGraphUtils.createFakeOutput(jobGraph, "FakeTailOutput", degreeOfParallelism);
    JobOutputVertex fakeSyncOutput = JobGraphUtils.createSingletonFakeOutput(jobGraph, "FakeSyncOutput");

    JobGraphUtils.connectLocal(input, head);
    //TODO implicit order should be documented/configured somehow
    JobGraphUtils.connectLocal(head, tail, DistributionPattern.BIPARTITE, ShipStrategy.PARTITION_HASH);
    JobGraphUtils.connectLocal(head, sync);
    JobGraphUtils.connectLocal(head, output);
    JobGraphUtils.connectLocal(tail, fakeTailOutput);
    JobGraphUtils.connectLocal(sync, fakeSyncOutput);

    head.setVertexToShareInstancesWith(tail);

    GlobalConfiguration.loadConfiguration("/home/ssc/Desktop/stratosphere/local-conf");
    Configuration conf = GlobalConfiguration.getConfiguration();

    JobGraphUtils.submit(jobGraph, conf);
  }

}