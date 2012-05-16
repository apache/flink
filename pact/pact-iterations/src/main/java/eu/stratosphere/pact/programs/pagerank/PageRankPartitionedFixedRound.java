package eu.stratosphere.pact.programs.pagerank;

import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.connectBoundedRoundsIterationLoop;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.connectJobVertices;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createInput;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createOutput;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createTask;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.getConfiguration;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.setMemorySize;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.submit;

import java.io.IOException;

import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.programs.inputs.AdjacencyListInput;
import eu.stratosphere.pact.programs.pagerank.tasks.InitialRankAssigner;
import eu.stratosphere.pact.programs.pagerank.tasks.RankOutput;
import eu.stratosphere.pact.programs.pagerank.tasks.RankReduceTask;
import eu.stratosphere.pact.programs.pagerank.tasks.VertexNeighbourContribCreatorNativeType;
import eu.stratosphere.pact.programs.pagerank.tasks.VertexRankMatchingBuildCaching;
import eu.stratosphere.pact.programs.pagerank.tasks.VertexRankTempTask;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

public class PageRankPartitionedFixedRound {
  public static void main(String[] args) throws JobGraphDefinitionException, IOException, JobExecutionException
  {
    if (args.length != 5) {
      System.out.println("Not correct parameters");
      System.exit(-1);
    }

    final int dop = Integer.valueOf(args[0]);
    final String input = args[1];
    final String output = args[2];
    final int spi = Integer.valueOf(args[3]);
    final int baseMemory = Integer.valueOf(args[4]);

    JobGraph graph = new JobGraph("Bulk PageRank Broadcast -- Optimized Twitter");

    //Create tasks
    JobInputVertex sourceVertex = createInput(AdjacencyListInput.class, input, graph, dop, spi);

    JobTaskVertex vertexRankContrib = createTask(VertexNeighbourContribCreatorNativeType.class, graph, dop, spi);
    vertexRankContrib.setVertexToShareInstancesWith(sourceVertex);

    JobTaskVertex initialRankAssigner = createTask(InitialRankAssigner.class, graph, dop, spi);
    initialRankAssigner.setVertexToShareInstancesWith(sourceVertex);

    JobTaskVertex tmpTask = createTask(VertexRankTempTask.class, graph, dop, spi);
    tmpTask.setVertexToShareInstancesWith(sourceVertex);
    setMemorySize(tmpTask, baseMemory*1 / 9);

    JobTaskVertex contribMatch = createTask(VertexRankMatchingBuildCaching.class, graph, dop, spi);
    contribMatch.setVertexToShareInstancesWith(sourceVertex);
    setMemorySize(contribMatch, baseMemory*6 /9);

    //Inner iteration loop tasks -- START
    JobTaskVertex rankReduce = createTask(RankReduceTask.class, graph, dop, spi);
    rankReduce.setVertexToShareInstancesWith(sourceVertex);
    setMemorySize(rankReduce, baseMemory*2 /9);
    //Inner iteration loop tasks -- END

    JobOutputVertex sinkVertex = createOutput(RankOutput.class, output, graph, dop, spi);
    sinkVertex.setVertexToShareInstancesWith(sourceVertex);

    //Connect tasks
    connectJobVertices(ShipStrategy.FORWARD, sourceVertex, initialRankAssigner, null, null);

    connectJobVertices(ShipStrategy.PARTITION_HASH, initialRankAssigner, tmpTask, null, null);

    connectJobVertices(ShipStrategy.FORWARD, sourceVertex, vertexRankContrib, null, null);

    connectBoundedRoundsIterationLoop(tmpTask, sinkVertex, new JobTaskVertex[] {rankReduce},
        new ShipStrategy[] {ShipStrategy.PARTITION_HASH}, rankReduce, contribMatch,
        ShipStrategy.FORWARD, 21, graph, false);

    connectJobVertices(ShipStrategy.PARTITION_HASH, vertexRankContrib, contribMatch, null, null);

    //Submit job
    submit(graph, getConfiguration());
  }
}
