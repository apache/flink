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
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.runtime.iterative.playing.JobGraphUtils;
import eu.stratosphere.pact.runtime.iterative.playing.PlayConstants;
import eu.stratosphere.pact.runtime.iterative.task.IterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationTailPactTask;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.ReduceDriver;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class IterativeMapReduce {

  public static void main(String[] args) throws Exception {

    int degreeOfParallelism = 2;
    int numSubTasksPerInstance = degreeOfParallelism;
    String inputPath = "file://" + PlayConstants.PLAY_DIR + "test-inputs/iterative-mapreduce/";
    String outputPath = "file:///tmp/stratosphere/iterations";
    String confPath = PlayConstants.PLAY_DIR + "local-conf";

    if (args.length == 5) {
      degreeOfParallelism = Integer.parseInt(args[0]);
      numSubTasksPerInstance = Integer.parseInt(args[1]);
      inputPath = args[2];
      outputPath = args[3];
      confPath = args[4];
    }

    JobGraph jobGraph = new JobGraph("Iterative MapReduce");

    JobInputVertex input = JobGraphUtils.createInput(TokenTokenInputFormat.class, inputPath, "FileInput", jobGraph,
        degreeOfParallelism, numSubTasksPerInstance);

    JobTaskVertex head = JobGraphUtils.createTask(IterationHeadPactTask.class, "BulkIterationHead", jobGraph,
        degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig headConfig = new TaskConfig(head.getConfiguration());
    headConfig.setDriver(MapDriver.class);
    headConfig.setStubClass(AppendTokenMapper.class);
    headConfig.setMemorySize(200 * JobGraphUtils.MEGABYTE);
    headConfig.setBackChannelMemoryFraction(0.8f);
    headConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(headConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });

    JobTaskVertex tail = JobGraphUtils.createTask(IterationTailPactTask.class, "BulkIterationTail", jobGraph,
        degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig tailConfig = new TaskConfig(tail.getConfiguration());
    tailConfig.setLocalStrategy(TaskConfig.LocalStrategy.SORT);
    tailConfig.setDriver(ReduceDriver.class);
    tailConfig.setStubClass(AppendTokenReducer.class);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(tailConfig.getConfigForInputParameters(0),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });
    tailConfig.setMemorySize(200 * JobGraphUtils.MEGABYTE);
    tailConfig.setNumFilehandles(2);
    tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, degreeOfParallelism);

    JobOutputVertex sync = JobGraphUtils.createSync(jobGraph, degreeOfParallelism);
    TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
    syncConfig.setNumberOfIterations(5);

    JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "FinalOutput", degreeOfParallelism,
        numSubTasksPerInstance);
    TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
    outputConfig.setStubClass(AppendTokenOutFormat.class);
    outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, outputPath);

    JobOutputVertex fakeTailOutput = JobGraphUtils.createFakeOutput(jobGraph, "FakeTailOutput", degreeOfParallelism,
        numSubTasksPerInstance);

    //TODO implicit order should be documented/configured somehow
    JobGraphUtils.connect(input, head, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategyType.FORWARD);
    JobGraphUtils.connect(head, tail, ChannelType.NETWORK, DistributionPattern.BIPARTITE,
        ShipStrategyType.PARTITION_HASH);
    JobGraphUtils.connect(head, sync, ChannelType.NETWORK, DistributionPattern.BIPARTITE,
        ShipStrategyType.FORWARD);
    JobGraphUtils.connect(head, output, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategyType.FORWARD);
    JobGraphUtils.connect(tail, fakeTailOutput, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategyType.FORWARD);

    input.setVertexToShareInstancesWith(head);
    tail.setVertexToShareInstancesWith(head);
    sync.setVertexToShareInstancesWith(head);
    output.setVertexToShareInstancesWith(head);
    fakeTailOutput.setVertexToShareInstancesWith(head);

    GlobalConfiguration.loadConfiguration(confPath);
    Configuration conf = GlobalConfiguration.getConfiguration();

    JobGraphUtils.submit(jobGraph, conf);
  }

}