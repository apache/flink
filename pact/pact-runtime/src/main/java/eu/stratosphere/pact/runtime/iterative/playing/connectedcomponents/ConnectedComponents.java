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

package eu.stratosphere.pact.runtime.iterative.playing.connectedcomponents;

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
import eu.stratosphere.pact.runtime.iterative.playing.JobGraphUtils;
import eu.stratosphere.pact.runtime.iterative.playing.PlayConstants;
import eu.stratosphere.pact.runtime.iterative.playing.pagerank.IdentityMap;
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationIntermediatePactTask;
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationTailPactTask;
import eu.stratosphere.pact.runtime.iterative.task.SolutionSetMatchDriver;
import eu.stratosphere.pact.runtime.iterative.task.WorksetIterationSolutionSetJoinTask;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.MatchDriver;
import eu.stratosphere.pact.runtime.task.ReduceDriver;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class ConnectedComponents {

  public static void main(String[] args) throws Exception {

    int degreeOfParallelism = 2;
    JobGraph jobGraph = new JobGraph("ConnectedComponents");

    JobInputVertex initialSolutionset = JobGraphUtils.createInput(LongLongInputFormat.class,
        "file://" + PlayConstants.PLAY_DIR + "test-inputs/connectedComponents/initialSolutionset",
        "InitialSolutionset", jobGraph, degreeOfParallelism);
    TaskConfig initialSolutionsetConfig = new TaskConfig(initialSolutionset.getConfiguration());
    initialSolutionsetConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(initialSolutionset.getConfiguration(),
        initialSolutionsetConfig.getPrefixForOutputParameters(0), new int[] { 0 }, new Class[] { PactLong.class });

    JobInputVertex initialWorkset = JobGraphUtils.createInput(LongLongInputFormat.class,
        "file://" + PlayConstants.PLAY_DIR + "test-inputs/connectedComponents/initialWorkset",
        "InitialWorkset", jobGraph, degreeOfParallelism);

    JobInputVertex graph = JobGraphUtils.createInput(LongLongInputFormat.class,
        "file://" + PlayConstants.PLAY_DIR + "test-inputs/connectedComponents/graph",
        "Graph", jobGraph, degreeOfParallelism);
    TaskConfig graphConfig = new TaskConfig(graph.getConfiguration());
    graphConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(graph.getConfiguration(),
        graphConfig.getPrefixForOutputParameters(0), new int[] { 0 }, new Class[] { PactLong.class });

    JobTaskVertex head = JobGraphUtils.createTask(BulkIterationHeadPactTask.class, "Head-Repartition", jobGraph,
        degreeOfParallelism);
    TaskConfig headConfig = new TaskConfig(head.getConfiguration());
    headConfig.setDriver(MapDriver.class);
    headConfig.setStubClass(IdentityMap.class);
    headConfig.setMemorySize(25 * JobGraphUtils.MEGABYTE);
    headConfig.setBackChannelMemoryFraction(0.5f);
    headConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(head.getConfiguration(),
        headConfig.getPrefixForOutputParameters(0), new int[] { 0 }, new Class[] { PactLong.class });
    headConfig.enableWorkset();


    JobTaskVertex intermediateMinimumComponentID = JobGraphUtils.createTask(BulkIterationIntermediatePactTask.class,
        "Intermediate-MinimumComponentID", jobGraph, degreeOfParallelism);
    TaskConfig intermediateMinimumComponentIDConfig = new TaskConfig(intermediateMinimumComponentID.getConfiguration());
    intermediateMinimumComponentIDConfig.setDriver(ReduceDriver.class);
    intermediateMinimumComponentIDConfig.setStubClass(MinimumComponentIDReduce.class);
    intermediateMinimumComponentIDConfig.setLocalStrategy(TaskConfig.LocalStrategy.SORT);
    intermediateMinimumComponentIDConfig.setMemorySize(3 * JobGraphUtils.MEGABYTE);
    intermediateMinimumComponentIDConfig.setNumFilehandles(2);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(intermediateMinimumComponentID.getConfiguration(),
        "pact.in.param.0.", new int[] { 0 }, new Class[] { PactLong.class });

    JobTaskVertex intermediateSolutionSetUpdate = JobGraphUtils.createTask(WorksetIterationSolutionSetJoinTask.class,
        "Intermediate-UpdateComponentID", jobGraph, degreeOfParallelism);
    TaskConfig intermediateSolutionSetUpdateConfig = new TaskConfig(intermediateSolutionSetUpdate.getConfiguration());
    intermediateSolutionSetUpdateConfig.setDriver(SolutionSetMatchDriver.class);
    intermediateSolutionSetUpdateConfig.setStubClass(UpdateCompontentIDMatch.class);
    intermediateSolutionSetUpdateConfig.setLocalStrategy(TaskConfig.LocalStrategy.HYBRIDHASH_SECOND);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(intermediateSolutionSetUpdateConfig.getConfiguration(),
        "pact.in.param.0.", new int[] { 0 }, new Class[] { PactLong.class });
    PactRecordComparatorFactory.writeComparatorSetupToConfig(intermediateSolutionSetUpdateConfig.getConfiguration(),
        "pact.in.param.1.", new int[] { 0 }, new Class[]{ PactLong.class });
    intermediateSolutionSetUpdateConfig.setMemorySize(20 * JobGraphUtils.MEGABYTE);

    JobTaskVertex tail = JobGraphUtils.createTask(BulkIterationTailPactTask.class,
        "Tail-NeighborComponentIDToWorkset", jobGraph, degreeOfParallelism);
    TaskConfig tailConfig = new TaskConfig(tail.getConfiguration());
    tailConfig.setDriver(MatchDriver.class);
    tailConfig.setStubClass(NeighborComponentIDToWorksetMatch.class);
    tailConfig.setLocalStrategy(TaskConfig.LocalStrategy.HYBRIDHASH_SECOND);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(tailConfig.getConfiguration(),
        "pact.in.param.0.", new int[]{0}, new Class[]{PactLong.class});
    PactRecordComparatorFactory.writeComparatorSetupToConfig(tailConfig.getConfiguration(),
        "pact.in.param.1.", new int[]{0}, new Class[]{PactLong.class});
    tailConfig.setMemorySize(20 * JobGraphUtils.MEGABYTE);
    tailConfig.setGateCached(0);
    tailConfig.setInputGateCacheMemoryFraction(0.5f);

    JobOutputVertex sync = JobGraphUtils.createSync(jobGraph, degreeOfParallelism);
    TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
    syncConfig.setNumberOfIterations(5);
    syncConfig.setConvergenceCriterion(WorksetEmptyConvergenceCriterion.class);

    JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "FinalOutput", degreeOfParallelism);
    TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
    outputConfig.setStubClass(ConnectedComponentsOutFormat.class);
    outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, "file:///tmp/stratosphere/iterations");

    JobOutputVertex fakeTailOutput = JobGraphUtils.createFakeOutput(jobGraph, "FakeTailOutput", degreeOfParallelism);

    JobGraphUtils.connect(initialWorkset, head, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategy.FORWARD);

    JobGraphUtils.connect(head, intermediateMinimumComponentID, ChannelType.NETWORK, DistributionPattern.BIPARTITE,
        ShipStrategy.PARTITION_HASH);
    intermediateMinimumComponentIDConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, degreeOfParallelism);

    JobGraphUtils.connect(intermediateMinimumComponentID, intermediateSolutionSetUpdate, ChannelType.NETWORK,
        DistributionPattern.POINTWISE, ShipStrategy.FORWARD);
    intermediateSolutionSetUpdateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

    JobGraphUtils.connect(initialSolutionset, intermediateSolutionSetUpdate, ChannelType.NETWORK,
        DistributionPattern.BIPARTITE, ShipStrategy.PARTITION_HASH);

    JobGraphUtils.connect(graph, tail, ChannelType.NETWORK, DistributionPattern.BIPARTITE,
        ShipStrategy.PARTITION_HASH);
    JobGraphUtils.connect(intermediateSolutionSetUpdate, tail, ChannelType.NETWORK, DistributionPattern.POINTWISE,
        ShipStrategy.FORWARD);
    tailConfig.setGateCached(0);
    tailConfig.setInputGateCacheMemoryFraction(0.5f);
    tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(1, 1);

    JobGraphUtils.connect(head, sync, ChannelType.NETWORK, DistributionPattern.POINTWISE, ShipStrategy.FORWARD);
    JobGraphUtils.connect(head, output, ChannelType.INMEMORY, DistributionPattern.POINTWISE, ShipStrategy.FORWARD);
    syncConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, degreeOfParallelism);

    JobGraphUtils.connect(tail, sync, ChannelType.NETWORK, DistributionPattern.POINTWISE, ShipStrategy.FORWARD);
    JobGraphUtils.connect(tail, fakeTailOutput, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategy.FORWARD);
    syncConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(1, degreeOfParallelism);

    initialWorkset.setVertexToShareInstancesWith(head);
    initialSolutionset.setVertexToShareInstancesWith(head);
    graph.setVertexToShareInstancesWith(head);
    intermediateMinimumComponentID.setVertexToShareInstancesWith(head);
    intermediateSolutionSetUpdate.setVertexToShareInstancesWith(head);
    tail.setVertexToShareInstancesWith(head);
    fakeTailOutput.setVertexToShareInstancesWith(head);
    output.setVertexToShareInstancesWith(head);
    sync.setVertexToShareInstancesWith(head);

    GlobalConfiguration.loadConfiguration(PlayConstants.PLAY_DIR + "local-conf");
    Configuration conf = GlobalConfiguration.getConfiguration();

    JobGraphUtils.submit(jobGraph, conf);
  }
}
