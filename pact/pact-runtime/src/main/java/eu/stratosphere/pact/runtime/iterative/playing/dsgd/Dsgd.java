package eu.stratosphere.pact.runtime.iterative.playing.dsgd;

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
import eu.stratosphere.pact.runtime.iterative.driver.RepeatableHashjoinMatchDriverWithCachedBuildside;
import eu.stratosphere.pact.runtime.iterative.playing.JobGraphUtils;
import eu.stratosphere.pact.runtime.iterative.playing.PlayConstants;
import eu.stratosphere.pact.runtime.iterative.playing.pagerank.DotProductMatch;
import eu.stratosphere.pact.runtime.iterative.playing.pagerank.IdentityMap;
import eu.stratosphere.pact.runtime.iterative.playing.pagerank.PageWithRankOutFormat;
import eu.stratosphere.pact.runtime.iterative.task.IterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationIntermediatePactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationTailPactTask;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class Dsgd {

  public static void main(String[] args) throws Exception {

    int degreeOfParallelism = 2;
    int numSubTasksPerInstance = degreeOfParallelism;
    String interactionsInputPath = "file://" + PlayConstants.PLAY_DIR + "test-inputs/dsgd/interactions";
    String userInputPath = "file://" + PlayConstants.PLAY_DIR + "test-inputs/dsgd/users";
    String itemInputPath = "file://" + PlayConstants.PLAY_DIR + "test-inputs/dsgd/items";
    String outputPath = "file:///tmp/stratosphere/iterations";
    String confPath = PlayConstants.PLAY_DIR + "local-conf";
    long memoryPerTask = 25;
    long memoryForMatch = 25;
    int numIterations = 5;

    JobGraph jobGraph = new JobGraph("DSGD");

    JobInputVertex interactions = JobGraphUtils.createInput(InteractionInputFormat.class, interactionsInputPath,
        "Interactions", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig interactionsConfig = new TaskConfig(interactions.getConfiguration());
    interactionsConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(interactionsConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });

    JobInputVertex userFeatures = JobGraphUtils.createInput(UserFeaturesInputFormat.class, userInputPath,
        "UserFeatures", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig userFeaturesConfig = new TaskConfig(userFeatures.getConfiguration());
    userFeaturesConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(userFeaturesConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });

    JobInputVertex itemFeatures = JobGraphUtils.createInput(ItemFeaturesInputFormat.class, itemInputPath,
        "ItemFeatures", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig itemFeaturesConfig = new TaskConfig(itemFeatures.getConfiguration());
    itemFeaturesConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(itemFeaturesConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });

    JobTaskVertex head = JobGraphUtils.createTask(IterationHeadPactTask.class, "IterationHead", jobGraph,
        degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig headConfig = new TaskConfig(head.getConfiguration());
    headConfig.setDriver(MapDriver.class);
    headConfig.setStubClass(IdentityMap.class);
    headConfig.setMemorySize(memoryPerTask * JobGraphUtils.MEGABYTE);
    headConfig.setBackChannelMemoryFraction(0.8f);
    headConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(headConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });

    JobTaskVertex interactionsMatch = JobGraphUtils.createTask(IterationIntermediatePactTask.class,
        "InteractionsMatch", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig interactionsMatchConfig = new TaskConfig(interactionsMatch.getConfiguration());
    interactionsMatchConfig.setDriver(RepeatableHashjoinMatchDriverWithCachedBuildside.class);
    interactionsMatchConfig.setStubClass(CollectInteractionMatch.class);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(interactionsMatchConfig.getConfigForInputParameters(0),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });
    PactRecordComparatorFactory.writeComparatorSetupToConfig(interactionsMatchConfig.getConfigForInputParameters(1),
        new int[] { 1 }, new Class[] { PactInteger.class }, new boolean[] { true });
    interactionsMatchConfig.setMemorySize(memoryForMatch * JobGraphUtils.MEGABYTE);
    interactionsMatchConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(interactionsMatchConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });

    JobTaskVertex updateParametersMatch = JobGraphUtils.createTask(IterationTailPactTask.class,
        "UpdateParametersMatch", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
    TaskConfig updateParametersMatchConfig = new TaskConfig(updateParametersMatch.getConfiguration());
    updateParametersMatchConfig.setDriver(RepeatableHashjoinMatchDriverWithCachedBuildside.class);
    updateParametersMatchConfig.setStubClass(UpdateParametersMatch.class);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(updateParametersMatchConfig.getConfigForInputParameters(0),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });
    PactRecordComparatorFactory.writeComparatorSetupToConfig(updateParametersMatchConfig.getConfigForInputParameters(1),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });
    updateParametersMatchConfig.setMemorySize(memoryPerTask * JobGraphUtils.MEGABYTE);
    updateParametersMatchConfig.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, 0);
    PactRecordComparatorFactory.writeComparatorSetupToConfig(updateParametersMatchConfig.getConfigForOutputParameters(0),
        new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });

    JobOutputVertex sync = JobGraphUtils.createSync(jobGraph, degreeOfParallelism);
    TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
    syncConfig.setNumberOfIterations(numIterations);

    JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "FinalOutput", degreeOfParallelism,
        numSubTasksPerInstance);
    TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
    outputConfig.setStubClass(PageWithRankOutFormat.class);
    outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, outputPath);

    JobOutputVertex fakeTailOutput = JobGraphUtils.createFakeOutput(jobGraph, "FakeTailOutput", degreeOfParallelism,
        numSubTasksPerInstance);


    JobGraphUtils.connect(itemFeatures, head, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategy.ShipStrategyType.FORWARD);

    JobGraphUtils.connect(head, interactionsMatch, ChannelType.NETWORK, DistributionPattern.BIPARTITE,
        ShipStrategy.ShipStrategyType.PARTITION_HASH);
    JobGraphUtils.connect(interactions, interactionsMatch, ChannelType.NETWORK, DistributionPattern.BIPARTITE,
        ShipStrategy.ShipStrategyType.PARTITION_HASH);
    interactionsMatchConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, degreeOfParallelism);

    JobGraphUtils.connect(interactionsMatch, updateParametersMatch, ChannelType.NETWORK,
        DistributionPattern.POINTWISE, ShipStrategy.ShipStrategyType.FORWARD);
    JobGraphUtils.connect(userFeatures, updateParametersMatch, ChannelType.NETWORK, DistributionPattern.BIPARTITE,
        ShipStrategy.ShipStrategyType.PARTITION_HASH);
    updateParametersMatchConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, degreeOfParallelism);

    JobGraphUtils.connect(head, sync, ChannelType.NETWORK, DistributionPattern.POINTWISE,
        ShipStrategy.ShipStrategyType.FORWARD);
    JobGraphUtils.connect(head, output, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategy.ShipStrategyType.FORWARD);

    JobGraphUtils.connect(updateParametersMatch, fakeTailOutput, ChannelType.INMEMORY, DistributionPattern.POINTWISE,
        ShipStrategy.ShipStrategyType.FORWARD);


    interactions.setVertexToShareInstancesWith(head);
    itemFeatures.setVertexToShareInstancesWith(head);
    interactionsMatch.setVertexToShareInstancesWith(head);
    updateParametersMatch.setVertexToShareInstancesWith(head);
    userFeatures.setVertexToShareInstancesWith(head);

    GlobalConfiguration.loadConfiguration(confPath);
    Configuration conf = GlobalConfiguration.getConfiguration();

    JobGraphUtils.submit(jobGraph, conf);
  }

}
