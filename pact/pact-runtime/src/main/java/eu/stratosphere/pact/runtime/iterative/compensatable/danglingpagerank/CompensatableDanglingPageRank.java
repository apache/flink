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

package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.runtime.iterative.driver.RepeatableHashjoinMatchDriverWithCachedBuildside;
import eu.stratosphere.pact.runtime.iterative.driver.SortingTempDriver;
import eu.stratosphere.pact.runtime.iterative.playing.JobGraphUtils;
import eu.stratosphere.pact.runtime.iterative.playing.PlayConstants;
import eu.stratosphere.pact.runtime.iterative.playing.pagerank.IdentityMap;
import eu.stratosphere.pact.runtime.iterative.playing.pagerank.PageWithRankOutFormat;
import eu.stratosphere.pact.runtime.iterative.task.IterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationIntermediatePactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationTailPactTask;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.CoGroupDriver;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.TempDriver;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class CompensatableDanglingPageRank {

  public static void main(String[] args) throws Exception {

    int degreeOfParallelism = 2;
    int numSubTasksPerInstance = degreeOfParallelism;
    String pageWithRankInputPath = "file://" + PlayConstants.PLAY_DIR + "test-inputs/danglingpagerank/pageWithRank";
    String adjacencyListInputPath = "file://" + PlayConstants.PLAY_DIR +
        "test-inputs/danglingpagerank/adjacencylists";
    String outputPath = "file:///tmp/stratosphere/iterations";
    String confPath = PlayConstants.PLAY_DIR + "local-conf";
    int memoryPerTask = 25;
    int memoryForMatch = memoryPerTask;
    int numIterations = 25;
    long numVertices = 5;
    long numDanglingVertices = 1;

    String failingWorkers = "1";
    int failingIteration = 2;
    double messageLoss = 0.75;

    if (args.length == 14) {
      degreeOfParallelism = Integer.parseInt(args[0]);
      numSubTasksPerInstance = Integer.parseInt(args[1]);
      pageWithRankInputPath = args[2];
      adjacencyListInputPath = args[3];
      outputPath = args[4];
      confPath = args[5];
      memoryPerTask = Integer.parseInt(args[6]);
      memoryForMatch = Integer.parseInt(args[7]);
      numIterations = Integer.parseInt(args[8]);
      numVertices = Long.parseLong(args[9]);
      numDanglingVertices = Long.parseLong(args[10]);
      failingWorkers = args[11];
      failingIteration = Integer.parseInt(args[12]);
      messageLoss = Double.parseDouble(args[13]);
    }

    JobGraph jobGraph = new JobGraph("CompensatableDanglingPageRank");

    JobInputVertex pageWithRankInput = JobGraphUtils.createInput(DanglingPageGenerateRankInputFormat.class,
        pageWithRankInputPath, "DanglingPageWithRankInput", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig pageWithRankInputConfig = new TaskConfig(pageWithRankInput.getConfiguration());
    pageWithRankInputConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(pageWithRankInputConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });
    pageWithRankInputConfig.setStubParameter("pageRank.numVertices", String.valueOf(numVertices));

    JobInputVertex adjacencyListInput = JobGraphUtils.createInput(AdjacencyListInputFormat.class,
        adjacencyListInputPath, "AdjancencyListInput", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig adjacencyListInputConfig = new TaskConfig(adjacencyListInput.getConfiguration());
    adjacencyListInputConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(adjacencyListInputConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });

    JobTaskVertex sortedPartitionedPageRank = JobGraphUtils.createTask(RegularPactTask.class,
        "SortedPartitionedPageRank", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig sortedPartitionedPageRankConfig = new TaskConfig(sortedPartitionedPageRank.getConfiguration());
    sortedPartitionedPageRankConfig.setDriver(SortingTempDriver.class);
    sortedPartitionedPageRankConfig.setStubClass(IdentityMap.class);
    sortedPartitionedPageRankConfig.setMemorySize(memoryPerTask * JobGraphUtils.MEGABYTE);
    sortedPartitionedPageRankConfig.setNumFilehandles(10);
    sortedPartitionedPageRankConfig.setComparatorFactoryForInput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(
        sortedPartitionedPageRankConfig.getConfigForInputParameters(0),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });

    JobTaskVertex head = JobGraphUtils.createTask(IterationHeadPactTask.class, "IterationHead", jobGraph,
        degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig headConfig = new TaskConfig(head.getConfiguration());
    headConfig.setDriver(MapDriver.class);
    headConfig.setStubClass(CompensatingMap.class);
    headConfig.setMemorySize(memoryPerTask * JobGraphUtils.MEGABYTE);
    headConfig.setBackChannelMemoryFraction(1f);
    headConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(headConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });
    headConfig.setStubParameter("pageRank.numVertices", String.valueOf(numVertices));
    headConfig.setStubParameter("compensation.failingWorker", failingWorkers);
    headConfig.setStubParameter("compensation.failingIteration", String.valueOf(failingIteration));
    headConfig.setStubParameter("compensation.messageLoss", String.valueOf(messageLoss));

    JobTaskVertex tempIntermediate = JobGraphUtils.createTask(IterationIntermediatePactTask.class, "TempIntermediate",
        jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig tempIntermediateConfig = new TaskConfig(tempIntermediate.getConfiguration());
    tempIntermediateConfig.setDriver(TempDriver.class);
    tempIntermediateConfig.setStubClass(IdentityMap.class);
    tempIntermediateConfig.setMemorySize(memoryPerTask * JobGraphUtils.MEGABYTE);

    JobTaskVertex intermediate = JobGraphUtils.createTask(IterationIntermediatePactTask.class,
        "IterationIntermediate", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig intermediateConfig = new TaskConfig(intermediate.getConfiguration());
    intermediateConfig.setDriver(RepeatableHashjoinMatchDriverWithCachedBuildside.class);
    intermediateConfig.setStubClass(CompensatableDotProductMatch.class);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(intermediateConfig.getConfigForInputParameters(0),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });
    PactRecordComparatorFactory.writeComparatorSetupToConfig(intermediateConfig.getConfigForInputParameters(1),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[]{true});
    intermediateConfig.setMemorySize(memoryForMatch * JobGraphUtils.MEGABYTE);
    intermediateConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(intermediateConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[]{true});
    intermediateConfig.setStubParameter("pageRank.numVertices", String.valueOf(numVertices));
    intermediateConfig.setStubParameter("compensation.failingWorker", failingWorkers);
    intermediateConfig.setStubParameter("compensation.failingIteration", String.valueOf(failingIteration));
    intermediateConfig.setStubParameter("compensation.messageLoss", String.valueOf(messageLoss));

    JobTaskVertex tail = JobGraphUtils.createTask(IterationTailPactTask.class, "IterationTail", jobGraph,
        degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig tailConfig = new TaskConfig(tail.getConfiguration());
    //TODO we need to combine!
    tailConfig.setDriver(CoGroupDriver.class);
    tailConfig.setLocalStrategy(TaskConfig.LocalStrategy.SORT_SECOND_MERGE);
    tailConfig.setStubClass(CompensatableDotProductCoGroup.class);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(tailConfig.getConfigForInputParameters(0), new int[] { 0 },
        new Class[] { PactLong.class }, new boolean[] { true });
    PactRecordComparatorFactory.writeComparatorSetupToConfig(tailConfig.getConfigForInputParameters(1), new int[] { 0 },
        new Class[]{PactLong.class}, new boolean[]{true});
    tailConfig.setMemorySize(memoryPerTask * JobGraphUtils.MEGABYTE);
    tailConfig.setNumFilehandles(10);
    tailConfig.setStubParameter("pageRank.numVertices", String.valueOf(numVertices));
    tailConfig.setStubParameter("pageRank.numDanglingVertices", String.valueOf(numDanglingVertices));
    tailConfig.setStubParameter("compensation.failingWorker", failingWorkers);
    tailConfig.setStubParameter("compensation.failingIteration", String.valueOf(failingIteration));
    tailConfig.setStubParameter("compensation.messageLoss", String.valueOf(messageLoss));

    JobOutputVertex sync = JobGraphUtils.createSync(jobGraph, degreeOfParallelism);
    TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
    syncConfig.setNumberOfIterations(numIterations);
    syncConfig.setConvergenceCriterion(DiffL1NormConvergenceCriterion.class);

    JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "FinalOutput", degreeOfParallelism,
        numSubTasksPerInstance);
    TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
    outputConfig.setStubClass(PageWithRankOutFormat.class);
    outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, outputPath);

    JobOutputVertex fakeTailOutput = JobGraphUtils.createFakeOutput(jobGraph, "FakeTailOutput", degreeOfParallelism,
        numSubTasksPerInstance);

    //TODO implicit order should be documented/configured somehow
    JobGraphUtils.connect(pageWithRankInput, sortedPartitionedPageRank, ChannelType.NETWORK,
        DistributionPattern.BIPARTITE, ShipStrategyType.PARTITION_HASH);
    JobGraphUtils.connect(sortedPartitionedPageRank, head, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategyType.FORWARD);

    JobGraphUtils.connect(head, intermediate, ChannelType.NETWORK, DistributionPattern.POINTWISE,
        ShipStrategyType.FORWARD);
    JobGraphUtils.connect(adjacencyListInput, intermediate, ChannelType.NETWORK, DistributionPattern.BIPARTITE,
        ShipStrategyType.PARTITION_HASH);
    intermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

    JobGraphUtils.connect(head, tempIntermediate, ChannelType.NETWORK, DistributionPattern.POINTWISE,
        ShipStrategyType.FORWARD);
    tempIntermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

    JobGraphUtils.connect(tempIntermediate, tail, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategyType.FORWARD);
    JobGraphUtils.connect(intermediate, tail, ChannelType.NETWORK, DistributionPattern.BIPARTITE,
        ShipStrategyType.PARTITION_HASH);
    tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);
    tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(1, degreeOfParallelism);


    JobGraphUtils.connect(head, sync, ChannelType.NETWORK, DistributionPattern.POINTWISE,
        ShipStrategyType.FORWARD);
    JobGraphUtils.connect(head, output, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategyType.FORWARD);
    JobGraphUtils.connect(tail, fakeTailOutput, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategyType.FORWARD);


    fakeTailOutput.setVertexToShareInstancesWith(tail);
    tail.setVertexToShareInstancesWith(head);
    pageWithRankInput.setVertexToShareInstancesWith(head);
    sortedPartitionedPageRank.setVertexToShareInstancesWith(head);
    adjacencyListInput.setVertexToShareInstancesWith(head);
    intermediate.setVertexToShareInstancesWith(head);
    output.setVertexToShareInstancesWith(head);
    sync.setVertexToShareInstancesWith(head);
    tempIntermediate.setVertexToShareInstancesWith(head);

    GlobalConfiguration.loadConfiguration(confPath);
    Configuration conf = GlobalConfiguration.getConfiguration();

    JobGraphUtils.submit(jobGraph, conf);
  }
}
