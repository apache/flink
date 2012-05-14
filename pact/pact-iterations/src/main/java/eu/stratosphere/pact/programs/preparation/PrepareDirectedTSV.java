package eu.stratosphere.pact.programs.preparation;

import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.connectJobVertices;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createInput;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.createOutput;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.getConfiguration;
import static eu.stratosphere.pact.iterative.nephele.util.NepheleUtil.submit;

import java.io.IOException;

import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.pact.programs.inputs.TSVInput;
import eu.stratosphere.pact.programs.preparation.tasks.AdjListOutput;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

public class PrepareDirectedTSV {
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
		
		System.out.println("DOP/SPI: " + dop + "//" + spi);
		JobGraph graph = new JobGraph("Bulk PageRank Broadcast -- Optimized Twitter");
		
		//Create tasks
		JobInputVertex sourceVertex = createInput(TSVInput.class, input, graph, dop, spi);
		
		JobOutputVertex sinkVertex = createOutput(AdjListOutput.class, output, graph, dop, spi);
		sinkVertex.setVertexToShareInstancesWith(sourceVertex);
		
		//Connect tasks
		connectJobVertices(ShipStrategy.FORWARD, sourceVertex, sinkVertex, null, null);
		
		//Submit job
		submit(graph, getConfiguration());
	}
}
