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

package eu.stratosphere.pact.runtime.iterative.playing.pagerank;

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
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.runtime.iterative.playing.JobGraphUtils;
import eu.stratosphere.pact.runtime.iterative.playing.PlayConstants;
import eu.stratosphere.pact.runtime.iterative.task.IterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationIntermediatePactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationTailPactTask;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.MatchDriver;
import eu.stratosphere.pact.runtime.task.ReduceDriver;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class PageRankWithTermination {

  public static void main(String[] args) throws Exception {

    int degreeOfParallelism = 2;
    JobGraph jobGraph = new JobGraph("PageRankWithTermination");

    JobInputVertex pageWithRankInput = JobGraphUtils.createInput(PageWithRankInputFormat.class,
        "file://" + PlayConstants.PLAY_DIR + "test-inputs/pagerank/pageWithRank", "PageWithRankInput", jobGraph,
        degreeOfParallelism);

    JobInputVertex transitionMatrixInput = JobGraphUtils.createInput(TransitionMatrixInputFormat.class,
        "file://" + PlayConstants.PLAY_DIR + "test-inputs/pagerank/transitionMatrix", "TransitionMatrixInput",
        jobGraph, degreeOfParallelism);
    TaskConfig transitionMatrixInputConfig = new TaskConfig(transitionMatrixInput.getConfiguration());
    transitionMatrixInputConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(transitionMatrixInputConfig.getConfigForOutputParameters(0),
        new int[] { 1 }, new Class[] { PactLong.class }, new boolean[] { true });

    JobTaskVertex head = JobGraphUtils.createTask(IterationHeadPactTask.class, "BulkIterationHead", jobGraph,
        degreeOfParallelism);
    TaskConfig headConfig = new TaskConfig(head.getConfiguration());
    headConfig.setDriver(MapDriver.class);
    headConfig.setStubClass(IdentityMap.class);
    headConfig.setMemorySize(3 * JobGraphUtils.MEGABYTE);
    headConfig.setBackChannelMemoryFraction(0.8f);

    JobTaskVertex intermediate = JobGraphUtils.createTask(IterationIntermediatePactTask.class,
        "BulkIterationIntermediate", jobGraph, degreeOfParallelism);
    TaskConfig intermediateConfig = new TaskConfig(intermediate.getConfiguration());
    intermediateConfig.setDriver(MatchDriver.class);
    intermediateConfig.setStubClass(DotProductMatch.class);
    intermediateConfig.setLocalStrategy(TaskConfig.LocalStrategy.HYBRIDHASH_FIRST);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(intermediateConfig.getConfigForInputParameters(0),
        new int[]{0}, new Class[]{PactLong.class}, new boolean[]{true});
    PactRecordComparatorFactory.writeComparatorSetupToConfig(intermediateConfig.getConfigForInputParameters(1),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });
    intermediateConfig.setMemorySize(20 * JobGraphUtils.MEGABYTE);
    intermediateConfig.setGateCached(1);
    intermediateConfig.setInputGateCacheMemoryFraction(0.5f);

    JobTaskVertex diffPerVertex = JobGraphUtils.createTask(IterationIntermediatePactTask.class, "DiffPerVertex",
        jobGraph, degreeOfParallelism);
    TaskConfig diffPerVertexConfig = new TaskConfig(diffPerVertex.getConfiguration());
    diffPerVertexConfig.setDriver(MatchDriver.class);
    diffPerVertexConfig.setStubClass(DiffPerVertexMatch.class);
    diffPerVertexConfig.setLocalStrategy(TaskConfig.LocalStrategy.HYBRIDHASH_FIRST);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(diffPerVertexConfig.getConfigForInputParameters(0),
        new int[]{0}, new Class[]{PactLong.class}, new boolean[]{true});
    PactRecordComparatorFactory.writeComparatorSetupToConfig(diffPerVertexConfig.getConfigForInputParameters(1),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });
    diffPerVertexConfig.setMemorySize(20 * JobGraphUtils.MEGABYTE);
    diffPerVertexConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(diffPerVertexConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactNull.class }, new boolean[] { true });

    JobTaskVertex tail = JobGraphUtils.createTask(IterationTailPactTask.class, "BulkIterationTail", jobGraph,
        degreeOfParallelism);
    TaskConfig tailConfig = new TaskConfig(tail.getConfiguration());
    tailConfig.setLocalStrategy(TaskConfig.LocalStrategy.SORT);
    tailConfig.setDriver(ReduceDriver.class);
    tailConfig.setStubClass(DotProductReducer.class);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(tailConfig.getConfigForInputParameters(0),
        new int[] { 0 }, new Class[] { PactLong.class }, new boolean[] { true });
    tailConfig.setMemorySize(3 * JobGraphUtils.MEGABYTE);
    tailConfig.setNumFilehandles(2);

    JobOutputVertex sync = JobGraphUtils.createSync(jobGraph, degreeOfParallelism);
    TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
    syncConfig.setNumberOfIterations(25);
    syncConfig.setConvergenceCriterion(L1NormConvergenceCriterion.class);

    JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "FinalOutput", degreeOfParallelism);
    TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
    outputConfig.setStubClass(PageWithRankOutFormat.class);
    outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, "file:///tmp/stratosphere/iterations");

    JobOutputVertex fakeTailOutput = JobGraphUtils.createFakeOutput(jobGraph, "FakeTailOutput", degreeOfParallelism);

    //TODO implicit order should be documented/configured somehow
    JobGraphUtils.connect(pageWithRankInput, head, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategy.ShipStrategyType.FORWARD);
    JobGraphUtils.connect(head, intermediate, ChannelType.NETWORK, DistributionPattern.BIPARTITE,
        ShipStrategy.ShipStrategyType.BROADCAST);
    JobGraphUtils.connect(transitionMatrixInput, intermediate, ChannelType.NETWORK, DistributionPattern.BIPARTITE,
        ShipStrategy.ShipStrategyType.PARTITION_HASH);
    intermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, degreeOfParallelism);

    JobGraphUtils.connect(intermediate, tail, ChannelType.NETWORK, DistributionPattern.POINTWISE,
        ShipStrategy.ShipStrategyType.FORWARD);
    tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

    JobGraphUtils.connect(head, diffPerVertex, ChannelType.NETWORK, DistributionPattern.BIPARTITE,
        ShipStrategy.ShipStrategyType.BROADCAST);
    JobGraphUtils.connect(tail, diffPerVertex, ChannelType.NETWORK, DistributionPattern.POINTWISE,
        ShipStrategy.ShipStrategyType.FORWARD);
    diffPerVertexConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, degreeOfParallelism);
    diffPerVertexConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(1, 1);

    JobGraphUtils.connect(head, sync, ChannelType.NETWORK, DistributionPattern.POINTWISE,
        ShipStrategy.ShipStrategyType.FORWARD);

    JobGraphUtils.connect(diffPerVertex, sync, ChannelType.NETWORK, DistributionPattern.POINTWISE,
        ShipStrategy.ShipStrategyType.FORWARD);
    syncConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(1, degreeOfParallelism);

    JobGraphUtils.connect(head, output, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategy.ShipStrategyType.FORWARD);
    JobGraphUtils.connect(tail, fakeTailOutput, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategy.ShipStrategyType.FORWARD);

    fakeTailOutput.setVertexToShareInstancesWith(tail);
    tail.setVertexToShareInstancesWith(head);
    pageWithRankInput.setVertexToShareInstancesWith(head);
    transitionMatrixInput.setVertexToShareInstancesWith(head);
    intermediate.setVertexToShareInstancesWith(head);
    output.setVertexToShareInstancesWith(head);
    sync.setVertexToShareInstancesWith(head);
    diffPerVertex.setVertexToShareInstancesWith(head);

    GlobalConfiguration.loadConfiguration(PlayConstants.PLAY_DIR + "local-conf");
    Configuration conf = GlobalConfiguration.getConfiguration();

    JobGraphUtils.submit(jobGraph, conf);
  }
}
