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
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.runtime.iterative.playing.JobGraphUtils;
import eu.stratosphere.pact.runtime.iterative.playing.PlayConstants;
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationIntermediatePactTask;
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationTailPactTask;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.MatchDriver;
import eu.stratosphere.pact.runtime.task.ReduceDriver;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class PageRank {

  public static void main(String[] args) throws Exception {

    int degreeOfParallelism = 2;
    JobGraph jobGraph = new JobGraph("PageRank");

    JobInputVertex pageWithRankInput = JobGraphUtils.createInput(PageWithRankInputFormat.class,
        "file://" + PlayConstants.PLAY_DIR + "test-inputs/pagerank/pageWithRank", "PageWithRankInput", jobGraph,
        degreeOfParallelism);

    JobInputVertex transitionMatrixInput = JobGraphUtils.createInput(TransitionMatrixInputFormat.class,
        "file://" + PlayConstants.PLAY_DIR + "test-inputs/pagerank/transitionMatrix", "TransitionMatrixInput",
        jobGraph, degreeOfParallelism);
    TaskConfig transitionMatrixInputConfig = new TaskConfig(transitionMatrixInput.getConfiguration());
    transitionMatrixInputConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(transitionMatrixInput.getConfiguration(),
        "pact.out.param.0.", new int[] { 1 }, new Class[] { PactLong.class });

    JobTaskVertex head = JobGraphUtils.createTask(BulkIterationHeadPactTask.class, "BulkIterationHead", jobGraph,
        degreeOfParallelism);
    TaskConfig headConfig = new TaskConfig(head.getConfiguration());
    headConfig.setDriver(MapDriver.class);
    headConfig.setStubClass(IdentityMap.class);
    headConfig.setMemorySize(3 * JobGraphUtils.MEGABYTE);
    headConfig.setBackChannelMemoryFraction(0.8f);
    headConfig.setNumberOfIterations(1);

    JobTaskVertex intermediate = JobGraphUtils.createTask(BulkIterationIntermediatePactTask.class,
        "BulkIterationIntermediate", jobGraph, degreeOfParallelism);
    TaskConfig intermediateConfig = new TaskConfig(intermediate.getConfiguration());
    intermediateConfig.setDriver(MatchDriver.class);
    intermediateConfig.setStubClass(DotProductMatch.class);
    intermediateConfig.setLocalStrategy(TaskConfig.LocalStrategy.HYBRIDHASH_FIRST);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(intermediateConfig.getConfiguration(),
        "pact.in.param.0.", new int[] { 0 }, new Class[] { PactLong.class });
    PactRecordComparatorFactory.writeComparatorSetupToConfig(intermediateConfig.getConfiguration(),
        "pact.in.param.1.", new int[] { 0 }, new Class[] { PactLong.class });
    intermediateConfig.setMemorySize(20 * JobGraphUtils.MEGABYTE);
    intermediateConfig.setGateCached(1);
    intermediateConfig.setInputGateCacheMemoryFraction(0.5f);

    JobTaskVertex tail = JobGraphUtils.createTask(BulkIterationTailPactTask.class, "BulkIterationTail", jobGraph,
        degreeOfParallelism);
    TaskConfig tailConfig = new TaskConfig(tail.getConfiguration());
    tailConfig.setLocalStrategy(TaskConfig.LocalStrategy.SORT);
    tailConfig.setDriver(ReduceDriver.class);
    tailConfig.setStubClass(DotProductReducer.class);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(tail.getConfiguration(), "pact.in.param.0.", new int[] { 0 },
        new Class[] { PactLong.class });
    tailConfig.setMemorySize(3 * JobGraphUtils.MEGABYTE);
    tailConfig.setNumFilehandles(2);

    JobOutputVertex sync = JobGraphUtils.createSync(jobGraph, degreeOfParallelism);

    JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "FinalOutput", degreeOfParallelism);
    TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
    outputConfig.setStubClass(PageWithRankOutFormat.class);
    outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, "file:///tmp/stratosphere/iterations");

    JobOutputVertex fakeTailOutput = JobGraphUtils.createFakeOutput(jobGraph, "FakeTailOutput", degreeOfParallelism);

    JobGraphUtils.connectLocal(pageWithRankInput, head);
    JobGraphUtils.connectLocal(head, intermediate, DistributionPattern.BIPARTITE, ShipStrategy.BROADCAST);
    JobGraphUtils.connectLocal(transitionMatrixInput, intermediate, DistributionPattern.BIPARTITE,
        ShipStrategy.PARTITION_HASH);
    intermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, degreeOfParallelism);

    JobGraphUtils.connectLocal(intermediate, tail, DistributionPattern.POINTWISE, ShipStrategy.FORWARD);
    tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

    //TODO implicit order should be documented/configured somehow
    JobGraphUtils.connectLocal(head, sync);
    JobGraphUtils.connectLocal(head, output);

    JobGraphUtils.connectLocal(tail, fakeTailOutput);

    fakeTailOutput.setVertexToShareInstancesWith(tail);
    tail.setVertexToShareInstancesWith(head);
    pageWithRankInput.setVertexToShareInstancesWith(head);
    transitionMatrixInput.setVertexToShareInstancesWith(head);
    intermediate.setVertexToShareInstancesWith(head);
    output.setVertexToShareInstancesWith(head);
    sync.setVertexToShareInstancesWith(head);

    GlobalConfiguration.loadConfiguration(PlayConstants.PLAY_DIR + "local-conf");
    Configuration conf = GlobalConfiguration.getConfiguration();

    JobGraphUtils.submit(jobGraph, conf);
  }
}
