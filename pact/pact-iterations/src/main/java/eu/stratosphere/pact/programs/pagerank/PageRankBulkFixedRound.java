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
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.programs.inputs.AdjListInput;
import eu.stratosphere.pact.programs.pagerank.tasks.ForwardingHead;
import eu.stratosphere.pact.programs.pagerank.tasks.InitialRankAssigner;
import eu.stratosphere.pact.programs.pagerank.tasks.RankOutput;
import eu.stratosphere.pact.programs.pagerank.tasks.RankReducePresorted;
import eu.stratosphere.pact.programs.pagerank.tasks.SortPairByNeighbour;
import eu.stratosphere.pact.programs.pagerank.tasks.VertexNeighbourContribCreator;
import eu.stratosphere.pact.programs.pagerank.tasks.VertexRankMatchBuild;
import eu.stratosphere.pact.programs.pagerank.tasks.VertexRankMatchProbeCaching;
import eu.stratosphere.pact.programs.pagerank.tasks.VertexRankTempTask;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

public class PageRankBulkFixedRound {	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws JobGraphDefinitionException, IOException, JobExecutionException
	{
		if(args.length != 5) {
			System.out.println("Not correct parameters");
			System.exit(-1);
		}
		
		final int dop = Integer.valueOf(args[0]);
		final String input = args[1];
		final String output = args[2];
		final int spi = Integer.valueOf(args[3]);
		final long baseMemory = Long.valueOf(args[4]);
		final Class<? extends Key> keyType = PactLong.class;
		
		JobGraph graph = new JobGraph("Bulk PageRank Broadcast -- Optimized Twitter");
		
		//Create tasks
		JobInputVertex sourceVertex = createInput(AdjListInput.class, input, graph, dop, spi);
		
		JobTaskVertex vertexRankContrib = createTask(VertexNeighbourContribCreator.class, graph, dop, spi);
		vertexRankContrib.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex initialRankAssigner = createTask(InitialRankAssigner.class, graph, dop, spi);
		initialRankAssigner.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex tmpTask = createTask(VertexRankTempTask.class, graph, dop, spi);
		tmpTask.setVertexToShareInstancesWith(sourceVertex);
		setMemorySize(tmpTask, baseMemory*1 / 9);
		
		JobTaskVertex sortedNeighbours = createTask(SortPairByNeighbour.class, graph, dop, spi);
		sortedNeighbours.setVertexToShareInstancesWith(sourceVertex);
		setMemorySize(sortedNeighbours, baseMemory*1 /9);
		
		JobTaskVertex forward = createTask(ForwardingHead.class, graph, dop, spi);
		forward.setVertexToShareInstancesWith(sourceVertex);
		setMemorySize(forward, baseMemory*1 / 9);
		
		//Inner iteration loop tasks -- START		
		JobTaskVertex buildMatch = createTask(VertexRankMatchBuild.class, graph, 4, 1);
		buildMatch.setVertexToShareInstancesWith(sourceVertex);
		setMemorySize(buildMatch, (baseMemory*4*spi) / 9);
		
		JobTaskVertex rankMatch = createTask(VertexRankMatchProbeCaching.class, graph, dop, spi);
		rankMatch.setVertexToShareInstancesWith(sourceVertex);
		setMemorySize(rankMatch, baseMemory*2 / 9);
		
		JobTaskVertex rankReduce = createTask(RankReducePresorted.class, graph, dop, spi);
		rankReduce.setVertexToShareInstancesWith(sourceVertex);
		//Inner iteration loop tasks -- END
		
		JobOutputVertex sinkVertex = createOutput(RankOutput.class, output, graph, dop, spi);
		sinkVertex.setVertexToShareInstancesWith(sourceVertex);
		
		//Connect tasks
		connectJobVertices(ShipStrategy.FORWARD, sourceVertex, initialRankAssigner, null, null);
		
		connectJobVertices(ShipStrategy.FORWARD, initialRankAssigner, tmpTask, null, null);
		
		connectJobVertices(ShipStrategy.FORWARD, sourceVertex, vertexRankContrib, null, null);
		
		connectJobVertices(ShipStrategy.PARTITION_HASH, vertexRankContrib, sortedNeighbours, 
				new int[] {1}, new Class[] {keyType});
		
		connectBoundedRoundsIterationLoop(tmpTask, sinkVertex, new JobTaskVertex[] {buildMatch}, 
				new ShipStrategy[] {ShipStrategy.BROADCAST}, rankReduce, forward, 
				ShipStrategy.FORWARD, 21, graph, false);
		
		connectJobVertices(ShipStrategy.BROADCAST, buildMatch, rankMatch, null, null);
		connectJobVertices(ShipStrategy.FORWARD, rankMatch, rankReduce, null, null);
		
		connectJobVertices(ShipStrategy.FORWARD, sortedNeighbours, rankMatch, null, null);
	
		//Submit job
		submit(graph, getConfiguration());
	}
}
