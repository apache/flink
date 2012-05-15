package eu.stratosphere.pact.iterative.nephele.util;

import java.io.IOException;
import java.util.Arrays;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.InputFormat;
import eu.stratosphere.pact.common.io.OutputFormat;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.iterative.nephele.bulk.BulkIterationHead;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;
import eu.stratosphere.pact.iterative.nephele.tasks.AsynchronousIterationTail;
import eu.stratosphere.pact.iterative.nephele.tasks.CounterTask;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationHead;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationStateSynchronizer;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationTail;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationTerminationChecker;
import eu.stratosphere.pact.runtime.task.DataSinkTask;
import eu.stratosphere.pact.runtime.task.DataSourceTask;
import eu.stratosphere.pact.runtime.task.util.NepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

public class NepheleUtil {
  // config dir parameters
  private static final String DEFAULT_CONFIG_DIRECTORY = "/home/mkaufmann/stratosphere-0.2/conf";
  private static final String ENV_CONFIG_DIRECTORY = "NEPHELE_CONF_DIR";
  public static final String TASK_MEMORY = "iter.task.memory";

  public static void setProperty(JobTaskVertex buildCache,
      String key, String value) {
    TaskConfig taskConfig = new TaskConfig(buildCache.getConfiguration());
    taskConfig.setStubParameter(key, value);

  }

  public static JobInputVertex createInput(Class<? extends InputFormat<? extends InputSplit>> format,
      String path, JobGraph graph, int dop, int spi) {
    JobInputVertex vertex = createInput(format, path, graph, dop);
    vertex.setNumberOfSubtasksPerInstance(spi);
    return vertex;
  }

  public static JobInputVertex createInput(Class<? extends InputFormat<? extends InputSplit>> format,
      String path, JobGraph graph, int dop) {
    JobInputVertex sourceVertex = new JobInputVertex("Input task", graph);
    sourceVertex.setInputClass(DataSourceTask.class);
    sourceVertex.setNumberOfSubtasks(dop);

    TaskConfig sourceConfig = new TaskConfig(sourceVertex.getConfiguration());
    sourceConfig.setStubClass(format);
    sourceConfig.setLocalStrategy(LocalStrategy.NONE);
    sourceConfig.setStubParameter(FileInputFormat.FILE_PARAMETER_KEY, path);

    return sourceVertex;
  }

  public static JobOutputVertex createOutput(Class<? extends OutputFormat> format,
      String path, JobGraph graph, int dop, int spi) {
    JobOutputVertex vertex = createOutput(format, path, graph, dop);
    vertex.setNumberOfSubtasksPerInstance(spi);
    return vertex;
  }

  public static JobOutputVertex createOutput(Class<? extends OutputFormat> format, String path,
      JobGraph graph, int dop) {
    JobOutputVertex sinkVertex = new JobOutputVertex("Output task", graph);
    sinkVertex.setOutputClass(DataSinkTask.class);
    sinkVertex.setNumberOfSubtasks(dop);
    sinkVertex.getConfiguration().setInteger(DataSinkTask.DEGREE_OF_PARALLELISM_KEY, dop);

    TaskConfig sinkConfig = new TaskConfig(sinkVertex.getConfiguration());
    sinkConfig.setStubClass(format);
    sinkConfig.setLocalStrategy(LocalStrategy.NONE);
    sinkConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, path);

    return sinkVertex;
  }

  public static JobOutputVertex createDummyOutput(JobGraph graph, int dop, int spi) {
    JobOutputVertex vertex = createDummyOutput(graph, dop);
    vertex.setNumberOfSubtasksPerInstance(spi);
    return vertex;
  }

  public static JobOutputVertex createDummyOutput(JobGraph graph, int dop) {
    JobOutputVertex sinkVertex = new JobOutputVertex("Dummy output task", graph);
    sinkVertex.setOutputClass(DummyNullOutput.class);
    sinkVertex.setNumberOfSubtasks(dop);
    sinkVertex.getConfiguration().setInteger(DataSinkTask.DEGREE_OF_PARALLELISM_KEY, dop);

    return sinkVertex;
  }

  public static JobTaskVertex createTask(Class<? extends AbstractMinimalTask> task, JobGraph graph, int dop, int spi) {
    JobTaskVertex vertex = createTask(task, graph, dop);
    vertex.setNumberOfSubtasksPerInstance(spi);
    return vertex;
  }

  public static JobTaskVertex createTask(Class<? extends AbstractMinimalTask> task, JobGraph graph, int dop) {
    JobTaskVertex taskVertex = new JobTaskVertex(task.getName(), graph);
    taskVertex.setTaskClass(task);
    taskVertex.setNumberOfSubtasks(dop);

    return taskVertex;
  }

  public static void connectJobVertices(ShipStrategy shipStrategy, AbstractJobVertex outputVertex,
      AbstractJobVertex inputVertex, int[] keyPos, Class<? extends Key>[] keyTypes) throws JobGraphDefinitionException {
    ChannelType channelType = null;

    switch (shipStrategy) {
    case FORWARD:
    case PARTITION_LOCAL_HASH:
      channelType = ChannelType.INMEMORY;
      break;
    case PARTITION_HASH:
    case PARTITION_RANGE:
    case BROADCAST:
    case SFR:
      channelType = ChannelType.NETWORK;
      break;
    default:
      throw new IllegalArgumentException("Unsupported ship-strategy: " + shipStrategy.name());
    }

    TaskConfig outputConfig = new TaskConfig(outputVertex.getConfiguration());
    TaskConfig inputConfig = new TaskConfig(inputVertex.getConfiguration());

    // connect child with inmemory channel
    outputVertex.connectTo(inputVertex, channelType, CompressionLevel.NO_COMPRESSION);
    // set ship strategy in vertex and child

    // set strategies in task configs
    if ( (keyPos == null | keyTypes == null) || (keyPos.length == 0 | keyTypes.length == 0)) {
      outputConfig.addOutputShipStrategy(shipStrategy);
    } else {
      outputConfig.addOutputShipStrategy(shipStrategy, keyPos, keyTypes);
    }
    inputConfig.addInputShipStrategy(shipStrategy, 0);
    System.out.println(outputVertex.getName() + " --> " + inputVertex.getName() + "::" + channelType);
  }

  public static void connectBoundedRoundsIterationLoop(AbstractJobVertex iterationInput, AbstractJobVertex iterationOutput,
      JobTaskVertex[] innerLoopStarts, JobTaskVertex innerLoopEnd, JobTaskVertex iterationHead,
      ShipStrategy iterationInputShipStrategy, int numRounds, JobGraph graph) throws JobGraphDefinitionException {
    ShipStrategy[] innerStartStrategies = null;
    if (innerLoopStarts != null) {
      innerStartStrategies = new ShipStrategy[innerLoopStarts.length];
      Arrays.fill(innerStartStrategies, ShipStrategy.FORWARD);
    }

    connectBoundedRoundsIterationLoop(iterationInput, iterationOutput, innerLoopStarts,
        innerStartStrategies, innerLoopEnd, iterationHead, iterationInputShipStrategy,
        numRounds, graph, false);
  }

  public static void connectAsyncBoundedRoundsIterationLoop(AbstractJobVertex iterationInput, AbstractJobVertex iterationOutput,
      JobTaskVertex[] innerLoopStarts, JobTaskVertex innerLoopEnd, JobTaskVertex iterationHead,
      ShipStrategy iterationInputShipStrategy, int numRounds, JobGraph graph) throws JobGraphDefinitionException {
    ShipStrategy[] innerStartStrategies = null;
    if (innerLoopStarts != null) {
      innerStartStrategies = new ShipStrategy[innerLoopStarts.length];
      Arrays.fill(innerStartStrategies, ShipStrategy.FORWARD);
    }

    connectBoundedRoundsIterationLoop(iterationInput, iterationOutput, innerLoopStarts,
        innerStartStrategies, innerLoopEnd, iterationHead, iterationInputShipStrategy,
        numRounds, graph, true);
  }

  public static void connectBoundedRoundsIterationLoop(AbstractJobVertex iterationInput,
      AbstractJobVertex iterationOutput,JobTaskVertex[] innerLoopStarts,
      ShipStrategy[] innerStartStrategies, JobTaskVertex innerLoopEnd, JobTaskVertex iterationHead,
      ShipStrategy iterationInputShipStrategy, int numRounds, JobGraph graph, boolean async)
          throws JobGraphDefinitionException {
    int dop = iterationInput.getNumberOfSubtasks();
    int spi = iterationInput.getNumberOfSubtasksPerInstance();


    iterationHead.getConfiguration().setBoolean(IterationHead.FIXED_POINT_TERMINATOR, false);
    iterationHead.getConfiguration().setInteger(IterationHead.NUMBER_OF_ITERATIONS, numRounds);

    //Create iteration tail
    JobTaskVertex iterationTail;
    if (async) {
      iterationTail = createTask(AsynchronousIterationTail.class, graph, dop, spi);
    } else {
      iterationTail = createTask(IterationTail.class, graph, dop, spi);
    }
    iterationTail.setVertexToShareInstancesWith(iterationInput);

    //Create synchronization task
    JobTaskVertex iterationStateSynchronizer = createTask(IterationStateSynchronizer.class, graph, 1);
    iterationStateSynchronizer.setVertexToShareInstancesWith(iterationInput);

    JobTaskVertex counter = createTask(CounterTask.class, graph, 1);
    counter.setVertexToShareInstancesWith(iterationInput);

    //Create dummy sink for synchronization point so that nephele does not complain
    JobOutputVertex dummySinkA = createDummyOutput(graph, 1);
    dummySinkA.setVertexToShareInstancesWith(iterationInput);

    JobOutputVertex dummySinkB = createDummyOutput(graph, 1);
    dummySinkB.setVertexToShareInstancesWith(iterationInput);

    //Create a connection between iteration input and iteration head
    connectJobVertices(iterationInputShipStrategy, iterationInput, iterationHead, null, null);

    //Create a connection between iteration head and iteration output
    connectJobVertices(ShipStrategy.FORWARD, iterationHead, iterationOutput, null, null);

    //Create direct connection between head and tail for nephele placement
    connectJobVertices(ShipStrategy.FORWARD, iterationHead, iterationTail, null, null);
    //Connect synchronization task with head and tail
    connectJobVertices(ShipStrategy.BROADCAST, iterationTail, iterationStateSynchronizer, null, null);
    connectJobVertices(ShipStrategy.BROADCAST, iterationHead, iterationStateSynchronizer, null, null);
    connectJobVertices(ShipStrategy.BROADCAST, iterationHead, counter, null, null);

    if (!(innerLoopStarts == null && innerLoopEnd == null)) {
      for (int i = 0; i < innerLoopStarts.length; i++) {
        connectJobVertices(innerStartStrategies[i], iterationHead, innerLoopStarts[i], null, null);
      }

      //Create a connection between inner loop end and iteration tail
      connectJobVertices(iterationInputShipStrategy, innerLoopEnd, iterationTail, null, null);
    } else {
      //Create a connection between inner loop end and iteration tail
      connectJobVertices(iterationInputShipStrategy, iterationHead, iterationTail, null, null);
    }

    //Connect synchronization task with dummy output
    connectJobVertices(ShipStrategy.FORWARD, iterationStateSynchronizer, dummySinkA, null, null);
    connectJobVertices(ShipStrategy.FORWARD, counter, dummySinkB, null, null);
  }

  /*
   * Iteration specific (make sure that iterationStart and iterationEnd share the same
   * instance and subtask id structure. The synchronizer is required, so that a new
   * iteration does not start before all other subtasks are finished.
   */
  public static void connectFixedPointIterationLoop(AbstractJobVertex iterationInput, AbstractJobVertex iterationOutput,
      JobTaskVertex[] innerLoopStarts, JobTaskVertex innerLoopEnd, JobTaskVertex terminationDataVertex,
      JobTaskVertex iterationHead, ShipStrategy iterationInputShipStrategy,
      Class<? extends TerminationDecider> decider, JobGraph graph) throws JobGraphDefinitionException {
    int dop = iterationInput.getNumberOfSubtasks();
    int spi = iterationInput.getNumberOfSubtasksPerInstance();

    iterationHead.getConfiguration().setBoolean(IterationHead.FIXED_POINT_TERMINATOR, true);

    //Create iteration tail
    JobTaskVertex iterationTail = createTask(IterationTail.class, graph, dop, spi);
    iterationTail.setVertexToShareInstancesWith(iterationInput);

    //Create synchronization task
    JobTaskVertex iterationStateSynchronizer = createTask(IterationStateSynchronizer.class, graph, 1);
    iterationStateSynchronizer.setVertexToShareInstancesWith(iterationInput);

    //Create termination decider
    JobTaskVertex terminationDeciderTask = createTask(IterationTerminationChecker.class, graph, 1);
    terminationDeciderTask.setVertexToShareInstancesWith(iterationInput);
    terminationDeciderTask.getConfiguration().setClass(IterationTerminationChecker.TERMINATION_DECIDER,
            decider);

    //Create dummy sink for synchronization point so that nephele does not complain
    JobOutputVertex dummySinkA = createDummyOutput(graph, 1);
    dummySinkA.setVertexToShareInstancesWith(iterationInput);

    //Create dummy sink for termination decider so that nephele does not complain
    JobOutputVertex dummySinkB = createDummyOutput(graph, 1);
    dummySinkB.setVertexToShareInstancesWith(iterationInput);


    //Create a connection between iteration input and iteration head
    connectJobVertices(iterationInputShipStrategy, iterationInput, iterationHead, null, null);

    //Create a connection between iteration head and iteration output
    connectJobVertices(ShipStrategy.FORWARD, iterationHead, iterationOutput, null, null);

    //Create a connection between termination output and termination decider
    connectJobVertices(ShipStrategy.BROADCAST, terminationDataVertex, terminationDeciderTask, null, null);

    //Create direct connection between head and tail for nephele placement
    connectJobVertices(ShipStrategy.FORWARD, iterationHead, iterationTail, null, null);
    //Connect synchronization task with head and tail
    connectJobVertices(ShipStrategy.BROADCAST, iterationTail, iterationStateSynchronizer, null, null);
    connectJobVertices(ShipStrategy.BROADCAST, iterationHead, iterationStateSynchronizer, null, null);
    //Connect termination decider to iteration head
    connectJobVertices(ShipStrategy.BROADCAST, iterationHead, terminationDeciderTask, null, null);

    //Create a connection between iteration head and inner loop starts
    for (JobTaskVertex innerLoopStart : innerLoopStarts) {
      connectJobVertices(ShipStrategy.FORWARD, iterationHead, innerLoopStart, null, null);
    }

    //Create a connection between inner loop end and iteration tail
        connectJobVertices(iterationInputShipStrategy, innerLoopEnd, iterationTail, null, null);

    //Connect synchronization task with dummy output
    connectJobVertices(ShipStrategy.FORWARD, iterationStateSynchronizer, dummySinkA, null, null);
    //Connect termination decider with dummy output
    connectJobVertices(ShipStrategy.FORWARD, terminationDeciderTask, dummySinkB, null, null);
  }

  /*
   * Iteration specific (make sure that iterationStart and iterationEnd share the same
   * instance and subtask id structure. The synchronizer is required, so that a new
   * iteration does not start before all other subtasks are finished.
   */
  public static void connectBulkIterationLoop(AbstractJobVertex iterationInput, AbstractJobVertex iterationOutput,
      JobTaskVertex[] innerLoopStarts, JobTaskVertex innerLoopEnd, JobTaskVertex terminationDataVertex,
      ShipStrategy iterationInputShipStrategy,
      Class<? extends TerminationDecider> decider, JobGraph graph) throws JobGraphDefinitionException {
    int dop = iterationInput.getNumberOfSubtasks();
    int spi = iterationInput.getNumberOfSubtasksPerInstance();

    //Create iteration head
    JobTaskVertex iterationHead = createTask(BulkIterationHead.class, graph, dop, spi);
    iterationHead.setVertexToShareInstancesWith(iterationInput);

    connectFixedPointIterationLoop(iterationInput, iterationOutput, innerLoopStarts, innerLoopEnd,
        terminationDataVertex, iterationHead, iterationInputShipStrategy, decider, graph);
  }

  public static void connectBulkIterationLoop(AbstractJobVertex iterationInput, AbstractJobVertex iterationOutput,
      JobTaskVertex[] innerLoopStarts, JobTaskVertex innerLoopEnd, int numIterations,
      ShipStrategy iterationInputShipStrategy, JobGraph graph) throws JobGraphDefinitionException {
    int dop = iterationInput.getNumberOfSubtasks();
    int spi = iterationInput.getNumberOfSubtasksPerInstance();

    //Create iteration head
    JobTaskVertex iterationHead = createTask(BulkIterationHead.class, graph, dop, spi);
    iterationHead.setVertexToShareInstancesWith(iterationInput);

    connectBoundedRoundsIterationLoop(iterationInput, iterationOutput, innerLoopStarts, innerLoopEnd,
        iterationHead, iterationInputShipStrategy, numIterations, graph);
  }


  public static Configuration getConfiguration() {
    String location = null;
    if (System.getenv(ENV_CONFIG_DIRECTORY) != null) {
      location = System.getenv(ENV_CONFIG_DIRECTORY);
    } else {
      location = DEFAULT_CONFIG_DIRECTORY;
    }
    System.out.println(location);

    GlobalConfiguration.loadConfiguration(location);
    Configuration config = GlobalConfiguration.getConfiguration();

    return config;
  }

  public static void submit(JobGraph graph, Configuration nepheleConfig) throws IOException, JobExecutionException {
    JobClient client = new JobClient(graph, nepheleConfig);
    client.submitJobAndWait();
  }

  public static void setMatchInformation(JobTaskVertex match, Class<? extends MatchStub> stubClass,
      int[] keyPosA, int[] keyPosB, Class<? extends Key>[] keyClasses) {
    TaskConfig config = new TaskConfig(match.getConfiguration());
    config.setStubClass(stubClass);
    config.setLocalStrategyKeyTypes(0, keyPosA);
    config.setLocalStrategyKeyTypes(1, keyPosB);
    config.setLocalStrategyKeyTypes(keyClasses);
  }

  public static void setReduceInformation(JobTaskVertex reduce,
      int[] keyPos, Class<? extends Key>[] keyClasses) {
    TaskConfig config = new TaskConfig(reduce.getConfiguration());
    config.setLocalStrategyKeyTypes(0, keyPos);
    config.setLocalStrategyKeyTypes(keyClasses);
  }

  public static void setMemorySize(JobTaskVertex task, long mem) {
    TaskConfig config = new TaskConfig(task.getConfiguration());
    config.setMemorySize(mem*1024*1024);
  }

  public static class DummyNullOutput extends AbstractOutputTask {

    private NepheleReaderIterator input;

    @Override
    public void registerInputOutput() {
      this.input = new NepheleReaderIterator(new MutableRecordReader<PactRecord>(this, DistributionPattern.POINTWISE);
    }

    @Override
    public void invoke() throws Exception {
      PactRecord rec = new PactRecord();
      while (input.next(rec)) {
        throw new RuntimeException("This task should not receive records");
      }
    }

  }
}
