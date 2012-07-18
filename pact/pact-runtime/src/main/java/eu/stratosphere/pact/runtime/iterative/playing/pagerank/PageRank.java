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
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationIntermediatePactTask;
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationSynchronizationPactTask;
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationTailPactTask;
import eu.stratosphere.pact.runtime.iterative.task.EmptyMapStub;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.MatchDriver;
import eu.stratosphere.pact.runtime.task.ReduceDriver;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class PageRank {

  public static void main(String[] args) throws Exception {

    int degreeOfParallelism = 1;
    JobGraph jobGraph = new JobGraph("PageRank");

    JobInputVertex pageWithRankInput = JobGraphUtils.createInput(PageWithRankInputFormat.class,
        "file:///home/ssc/Desktop/stratosphere/test-inputs/pagerank/pageWithRank", "PageWithRankInput", jobGraph,
        degreeOfParallelism);

    JobInputVertex transitionMatrixInput = JobGraphUtils.createInput(TransitionMatrixInputFormat.class,
        "file:///home/ssc/Desktop/stratosphere/test-inputs/pagerank/transitionMatrix", "TransitionMatrixInput",
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

    JobTaskVertex sync = JobGraphUtils.createSingletonTask(BulkIterationSynchronizationPactTask.class, "BulkIterationSync",
        jobGraph);
    TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
    syncConfig.setDriver(MapDriver.class);
    syncConfig.setStubClass(EmptyMapStub.class);

    JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "FinalOutput", degreeOfParallelism);
    TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
    outputConfig.setStubClass(PageWithRankOutFormat.class);
    outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, "file:///tmp/stratosphere/iterations");

    JobOutputVertex fakeTailOutput = JobGraphUtils.createFakeOutput(jobGraph, "FakeTailOutput", degreeOfParallelism);
    JobOutputVertex fakeSyncOutput = JobGraphUtils.createSingletonFakeOutput(jobGraph, "FakeSyncOutput");

    JobGraphUtils.connectLocal(pageWithRankInput, head);
    JobGraphUtils.connectLocal(head, intermediate, DistributionPattern.BIPARTITE, ShipStrategy.BROADCAST);
    JobGraphUtils.connectLocal(transitionMatrixInput, intermediate, DistributionPattern.BIPARTITE,
        ShipStrategy.PARTITION_HASH);
    intermediateConfig.setGateIterativeAndSetNumberOfEventsUntilInterrupt(0, degreeOfParallelism);

    JobGraphUtils.connectLocal(intermediate, tail, DistributionPattern.POINTWISE, ShipStrategy.FORWARD);
    tailConfig.setGateIterativeAndSetNumberOfEventsUntilInterrupt(0, 1);

    //TODO implicit order should be documented/configured somehow
    JobGraphUtils.connectLocal(head, sync);
    syncConfig.setGateIterativeAndSetNumberOfEventsUntilInterrupt(0, degreeOfParallelism);

    JobGraphUtils.connectLocal(head, output);
    JobGraphUtils.connectLocal(tail, fakeTailOutput);
    JobGraphUtils.connectLocal(sync, fakeSyncOutput);

    head.setVertexToShareInstancesWith(tail);

    GlobalConfiguration.loadConfiguration("/home/ssc/Desktop/stratosphere/local-conf");
    Configuration conf = GlobalConfiguration.getConfiguration();

    JobGraphUtils.submit(jobGraph, conf);
  }
}
