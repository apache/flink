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

package eu.stratosphere.pact.runtime.iterative.playing.connectedcomponents.rowpartitioned;

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
import eu.stratosphere.pact.runtime.iterative.convergence.SolutionsetEmptyConvergenceCriterion;
import eu.stratosphere.pact.runtime.iterative.driver.RepeatableHashjoinMatchDriverWithCachedBuildside;
import eu.stratosphere.pact.runtime.iterative.driver.SolutionsetMatchDriver;
import eu.stratosphere.pact.runtime.iterative.playing.JobGraphUtils;
import eu.stratosphere.pact.runtime.iterative.playing.PlayConstants;
import eu.stratosphere.pact.runtime.iterative.playing.connectedcomponents.ConnectedComponentsOutFormat;
import eu.stratosphere.pact.runtime.iterative.playing.connectedcomponents.DuplicateLongInputFormat;
import eu.stratosphere.pact.runtime.iterative.playing.connectedcomponents.LongLongInputFormat;
import eu.stratosphere.pact.runtime.iterative.playing.connectedcomponents.MinimumComponentIDReduce;
import eu.stratosphere.pact.runtime.iterative.playing.connectedcomponents.NeighborComponentIDToWorksetMatch;
import eu.stratosphere.pact.runtime.iterative.playing.connectedcomponents.UpdateCompontentIDMatch;
import eu.stratosphere.pact.runtime.iterative.playing.pagerank.IdentityMap;
import eu.stratosphere.pact.runtime.iterative.task.IterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationIntermediatePactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationTailPactTask;
import eu.stratosphere.pact.runtime.iterative.task.WorksetIterationSolutionsetJoinTask;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.ReduceDriver;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class RowPartitionedConnectedComponents {

  public static void main(String[] args) throws Exception {

    int degreeOfParallelism = 2;
    int numSubTasksPerInstance = degreeOfParallelism;
    String initialSolutionSetPath = "file://" + PlayConstants.PLAY_DIR +
        "test-inputs/connectedComponents/initialSolutionset";
    String initialWorksetPath = "file://" + PlayConstants.PLAY_DIR + "test-inputs/connectedComponents/graph";
    String graphPath = "file://" + PlayConstants.PLAY_DIR + "test-inputs/connectedComponents/graph-adjacency";
    String outputPath = "file:///tmp/stratosphere/iterations";
    String confPath = PlayConstants.PLAY_DIR + "local-conf";
    long memoryPerTask = 10;
    long memoryForMatch = memoryPerTask;


    if (args.length == 9) {
      degreeOfParallelism = Integer.parseInt(args[0]);
      numSubTasksPerInstance = Integer.parseInt(args[1]);
      initialSolutionSetPath = args[2];
      initialWorksetPath = args[3];
      graphPath = args[4];
      outputPath = args[5];
      confPath = args[6];
      memoryPerTask = Integer.parseInt(args[7]);
      memoryForMatch = Integer.parseInt(args[8]);
    }

    JobGraph jobGraph = new JobGraph("RowPartitionedConnectedComponents");

    JobInputVertex initialSolutionset = JobGraphUtils.createInput(DuplicateLongInputFormat.class,
        initialSolutionSetPath,  "InitialSolutionset", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig initialSolutionsetConfig = new TaskConfig(initialSolutionset.getConfiguration());
    initialSolutionsetConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(initialSolutionsetConfig.getConfigForOutputParameters(0),
      new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });

    JobInputVertex initialWorkset = JobGraphUtils.createInput(LongLongInputFormat.class, initialWorksetPath,
        "InitialWorkset", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig initialWorksetConfig = new TaskConfig(initialWorkset.getConfiguration());
    initialWorksetConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(initialWorksetConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });

    JobInputVertex graph = JobGraphUtils.createInput(AdjacencyListInputFormat.class, graphPath, "Graph",
        jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig graphConfig = new TaskConfig(graph.getConfiguration());
    graphConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(graphConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });

    JobTaskVertex head = JobGraphUtils.createTask(IterationHeadPactTask.class, "Head-Repartition", jobGraph,
        degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig headConfig = new TaskConfig(head.getConfiguration());
    headConfig.setDriver(MapDriver.class);
    headConfig.setStubClass(IdentityMap.class);
    headConfig.setMemorySize((long) ((memoryForMatch * 1.5) * JobGraphUtils.MEGABYTE));
    headConfig.setBackChannelMemoryFraction(0.3f);
    headConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(headConfig.getConfigForOutputParameters(0), new int[]{ 0 },
        new Class[] { PactLong.class }, new boolean[] { true });

    headConfig.enableWorkset();
    headConfig.setWorksetHashjoinMemoryFraction(0.7f);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(
        headConfig.getConfigurationForWorksetHashjoinBuildside(), new int[] { 0 }, new Class[] { PactLong.class },
        new boolean[] { true });
    PactRecordComparatorFactory.writeComparatorSetupToConfig(
        headConfig.getConfigurationForWorksetHashjoinProbeside(), new int[] { 0 }, new Class[] { PactLong.class },
        new boolean[] { true });

    JobTaskVertex intermediateMinimumComponentID = JobGraphUtils.createTask(IterationIntermediatePactTask.class,
        "Intermediate-MinimumComponentID", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig intermediateMinimumComponentIDConfig = new TaskConfig(intermediateMinimumComponentID.getConfiguration());
    intermediateMinimumComponentIDConfig.setDriver(ReduceDriver.class);
    intermediateMinimumComponentIDConfig.setStubClass(MinimumComponentIDReduce.class);
    intermediateMinimumComponentIDConfig.setLocalStrategy(TaskConfig.LocalStrategy.COMBININGSORT);
    intermediateMinimumComponentIDConfig.setMemorySize(memoryPerTask * JobGraphUtils.MEGABYTE);
    intermediateMinimumComponentIDConfig.setNumFilehandles(10);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(
        intermediateMinimumComponentIDConfig.getConfigForInputParameters(0), new int[] { 0 },
        new Class[] { PactLong.class }, new boolean[] { true });

    JobTaskVertex intermediateSolutionSetUpdate = JobGraphUtils.createTask(WorksetIterationSolutionsetJoinTask.class,
        "Intermediate-UpdateComponentID", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig intermediateSolutionSetUpdateConfig = new TaskConfig(intermediateSolutionSetUpdate.getConfiguration());
    intermediateSolutionSetUpdateConfig.setDriver(SolutionsetMatchDriver.class);
    intermediateSolutionSetUpdateConfig.setStubClass(UpdateCompontentIDMatch.class);
    intermediateSolutionSetUpdateConfig.setLocalStrategy(TaskConfig.LocalStrategy.HYBRIDHASH_SECOND);
    //TODO we should not have to configure this, as the head sets up the hash-join for us
    PactRecordComparatorFactory.writeComparatorSetupToConfig(
        intermediateSolutionSetUpdateConfig.getConfigForInputParameters(0), new int[] { 0 },
        new Class[] { PactLong.class }, new boolean[] { true });
    PactRecordComparatorFactory.writeComparatorSetupToConfig(
        intermediateSolutionSetUpdateConfig.getConfigForInputParameters(1), new int[] { 0 },
        new Class[] { PactLong.class }, new boolean[] { true });

    JobTaskVertex tail = JobGraphUtils.createTask(IterationTailPactTask.class, "Tail-NeighborsComponentIDToWorkset",
        jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig tailConfig = new TaskConfig(tail.getConfiguration());
    tailConfig.setDriver(RepeatableHashjoinMatchDriverWithCachedBuildside.class);
    tailConfig.setStubClass(NeighborsComponentIDToWorksetMatch.class);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(tailConfig.getConfigForInputParameters(0),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });
    PactRecordComparatorFactory.writeComparatorSetupToConfig(tailConfig.getConfigForInputParameters(1),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });
    tailConfig.setMemorySize(memoryForMatch * JobGraphUtils.MEGABYTE);

    JobOutputVertex sync = JobGraphUtils.createSync(jobGraph, degreeOfParallelism);
    TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
    syncConfig.setNumberOfIterations(100);
    syncConfig.setConvergenceCriterion(SolutionsetEmptyConvergenceCriterion.class);

    JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "FinalOutput", degreeOfParallelism,
        numSubTasksPerInstance);
    TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
    outputConfig.setStubClass(ConnectedComponentsOutFormat.class);
    outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, outputPath);

    JobOutputVertex fakeTailOutput = JobGraphUtils.createFakeOutput(jobGraph, "FakeTailOutput", degreeOfParallelism,
        numSubTasksPerInstance);

    JobGraphUtils.connect(initialWorkset, head, ChannelType.NETWORK, DistributionPattern.POINTWISE,
        ShipStrategyType.FORWARD);

    JobGraphUtils.connect(head, intermediateMinimumComponentID, ChannelType.NETWORK, DistributionPattern.BIPARTITE,
        ShipStrategyType.PARTITION_HASH);
    intermediateMinimumComponentIDConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, degreeOfParallelism);

    JobGraphUtils.connect(intermediateMinimumComponentID, intermediateSolutionSetUpdate, ChannelType.NETWORK,
        DistributionPattern.POINTWISE, ShipStrategyType.FORWARD);
    intermediateSolutionSetUpdateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

    JobGraphUtils.connect(initialSolutionset, intermediateSolutionSetUpdate, ChannelType.NETWORK,
        DistributionPattern.BIPARTITE, ShipStrategyType.PARTITION_HASH);

    JobGraphUtils.connect(intermediateSolutionSetUpdate, tail, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategyType.FORWARD);
    JobGraphUtils.connect(graph, tail, ChannelType.NETWORK, DistributionPattern.BIPARTITE,
        ShipStrategyType.PARTITION_HASH);
    tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

    JobGraphUtils.connect(head, sync, ChannelType.NETWORK, DistributionPattern.POINTWISE,
        ShipStrategyType.FORWARD);
    JobGraphUtils.connect(head, output, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategyType.FORWARD);
    syncConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, degreeOfParallelism);

    JobGraphUtils.connect(tail, fakeTailOutput, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategyType.FORWARD);

    initialSolutionset.setVertexToShareInstancesWith(head);
    initialWorkset.setVertexToShareInstancesWith(head);
    graph.setVertexToShareInstancesWith(head);
    intermediateMinimumComponentID.setVertexToShareInstancesWith(head);
    intermediateSolutionSetUpdate.setVertexToShareInstancesWith(head);
    tail.setVertexToShareInstancesWith(head);
    fakeTailOutput.setVertexToShareInstancesWith(head);
    output.setVertexToShareInstancesWith(head);
    sync.setVertexToShareInstancesWith(head);

    GlobalConfiguration.loadConfiguration(confPath);
    Configuration conf = GlobalConfiguration.getConfiguration();

    JobGraphUtils.submit(jobGraph, conf);
  }
}
