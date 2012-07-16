package eu.stratosphere.pact.runtime.iterative.playing.pagerank;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.runtime.iterative.playing.JobGraphUtils;
import eu.stratosphere.pact.runtime.iterative.playing.iterativemapreduce.AppendTokenOutFormat;
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationSynchronizationPactTask;
import eu.stratosphere.pact.runtime.iterative.task.BulkIterationTailPactTask;
import eu.stratosphere.pact.runtime.iterative.task.EmptyMapStub;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy;
import eu.stratosphere.pact.runtime.task.DataSourceTask;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.MatchDriver;
import eu.stratosphere.pact.runtime.task.ReduceDriver;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class PageRank {

  public static void main(String[] args) throws Exception {

    int degreeOfParallelism = 1;
    JobGraph jobGraph = new JobGraph();

    JobInputVertex pageWithRankInput = new JobInputVertex("PageWithRankInput", jobGraph);
    Class<AbstractInputTask<?>> clazz = (Class<AbstractInputTask<?>>) (Class<?>) DataSourceTask.class;
    pageWithRankInput.setInputClass(clazz);
    pageWithRankInput.setNumberOfSubtasks(degreeOfParallelism);
    pageWithRankInput.setNumberOfSubtasksPerInstance(degreeOfParallelism);
    TaskConfig pageWithRankInputConfig = new TaskConfig(pageWithRankInput.getConfiguration());
    pageWithRankInputConfig.setStubClass(PageWithRankInputFormat.class);
    pageWithRankInputConfig.setLocalStrategy(TaskConfig.LocalStrategy.NONE);
    pageWithRankInputConfig.setStubParameter(FileInputFormat.FILE_PARAMETER_KEY,
        "file:///home/ssc/Desktop/iterative-mapreduce/");

    JobInputVertex transitionMatrixInput = new JobInputVertex("TransitionMatrixInput", jobGraph);
    transitionMatrixInput.setNumberOfSubtasks(degreeOfParallelism);
    transitionMatrixInput.setNumberOfSubtasksPerInstance(degreeOfParallelism);
    TaskConfig transitionMatrixInputConfig = new TaskConfig(transitionMatrixInput.getConfiguration());
    transitionMatrixInputConfig.setStubClass(TransitionMatrixInputFormat.class);
    transitionMatrixInputConfig.setLocalStrategy(TaskConfig.LocalStrategy.NONE);
    transitionMatrixInputConfig.setStubParameter(FileInputFormat.FILE_PARAMETER_KEY,
        "file:///home/ssc/Desktop/iterative-mapreduce/");

    JobTaskVertex head = JobGraphUtils.createTask(BulkIterationHeadPactTask.class, "BulkIterationHead", jobGraph,
        degreeOfParallelism);
    TaskConfig headConfig = new TaskConfig(head.getConfiguration());
    headConfig.setDriver(MatchDriver.class);
    headConfig.setStubClass(DotProductMatch.class);
    headConfig.setLocalStrategy(TaskConfig.LocalStrategy.HYBRIDHASH_FIRST);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(head.getConfiguration(), "pact.in.param.0.", new int[] { 0 },
        new Class[] { PactLong.class });
    PactRecordComparatorFactory.writeComparatorSetupToConfig(head.getConfiguration(), "pact.in.param.1.", new int[] { 0 },
        new Class[] { PactLong.class });
    headConfig.setMemorySize(10 * JobGraphUtils.MEGABYTE);
    headConfig.setBackChannelMemoryFraction(0.8f);
    headConfig.setNumberOfIterations(3);

    JobTaskVertex tail = JobGraphUtils.createTask(BulkIterationTailPactTask.class, "BulkIterationTail", jobGraph,
        degreeOfParallelism);
    TaskConfig tailConfig = new TaskConfig(tail.getConfiguration());
    tailConfig.setLocalStrategy(TaskConfig.LocalStrategy.SORT);
    tailConfig.setDriver(ReduceDriver.class);
    tailConfig.setStubClass(DotProductReducer.class);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(tail.getConfiguration(), "pact.in.param.0.", new int[] { 0 },
        new Class[] { PactString.class });
    tailConfig.setMemorySize(3 * JobGraphUtils.MEGABYTE);
    tailConfig.setNumFilehandles(2);
    tailConfig.setNumberOfEventsUntilInterruptInIterativeGate(0, degreeOfParallelism);

    JobTaskVertex sync = JobGraphUtils.createSingletonTask(BulkIterationSynchronizationPactTask.class, "BulkIterationSync",
        jobGraph);
    TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
    syncConfig.setDriver(MapDriver.class);
    syncConfig.setStubClass(EmptyMapStub.class);
    syncConfig.setNumberOfEventsUntilInterruptInIterativeGate(0, degreeOfParallelism);

    JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "FinalOutput", degreeOfParallelism);
    TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
    outputConfig.setStubClass(AppendTokenOutFormat.class);
    outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, "file:///tmp/stratosphere/iterations");

    JobOutputVertex fakeTailOutput = JobGraphUtils.createFakeOutput(jobGraph, "FakeTailOutput", degreeOfParallelism);
    JobOutputVertex fakeSyncOutput = JobGraphUtils.createSingletonFakeOutput(jobGraph, "FakeSyncOutput");

    JobGraphUtils.connectLocal(pageWithRankInput, head, pageWithRankInputConfig, DistributionPattern.BIPARTITE,
        ShipStrategy.BROADCAST);
    JobGraphUtils.connectLocal(transitionMatrixInput, head, transitionMatrixInputConfig);
    //TODO implicit order should be documented/configured somehow
    JobGraphUtils.connectLocal(head, tail, headConfig, DistributionPattern.BIPARTITE, ShipStrategy.FORWARD);
    JobGraphUtils.connectLocal(head, sync, headConfig);
    JobGraphUtils.connectLocal(head, output, headConfig);
    JobGraphUtils.connectLocal(tail, fakeTailOutput, tailConfig);
    JobGraphUtils.connectLocal(sync, fakeSyncOutput, syncConfig);

    head.setVertexToShareInstancesWith(tail);

    GlobalConfiguration.loadConfiguration(
        "/home/ssc/Entwicklung/projects/stratosphere-iterations/stratosphere-dist/src/main/stratosphere-bin/conf");
    Configuration conf = GlobalConfiguration.getConfiguration();

    JobGraphUtils.submit(jobGraph, conf);

  }
}
