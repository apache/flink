package eu.stratosphere.pact.programs.connected;

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
import eu.stratosphere.pact.iterative.nephele.util.NepheleUtil;
import eu.stratosphere.pact.programs.connected.tasks.AsynchronousUpdateableMatchingOptimized;
import eu.stratosphere.pact.programs.connected.tasks.ConvertToTransitiveClosureTypes;
import eu.stratosphere.pact.programs.connected.tasks.InitialStateComponents;
import eu.stratosphere.pact.programs.connected.tasks.InitialUpdates;
import eu.stratosphere.pact.programs.connected.tasks.UpdateTempTask;
import eu.stratosphere.pact.programs.inputs.AdjListInput;
import eu.stratosphere.pact.programs.inputs.NullOutput;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

public class AsynchronousConnectedComponentsOptimized {	
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
		final int baseMemory = Integer.valueOf(args[4]);
		
		JobGraph graph = new JobGraph("Connected Components");
		
		//Create tasks
		JobInputVertex sourceVertex = createInput(AdjListInput.class, input, graph, dop, spi);
		
		JobTaskVertex convert = createTask(ConvertToTransitiveClosureTypes.class, graph, dop, spi);
		convert.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex initialState = createTask(InitialStateComponents.class, graph, dop, spi);
		initialState.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex initialUpdateAssigner = createTask(InitialUpdates.class, graph, dop, spi);
		initialUpdateAssigner.setVertexToShareInstancesWith(sourceVertex);
		
		JobTaskVertex tmpTask = createTask(UpdateTempTask.class, graph, dop);
		tmpTask.setVertexToShareInstancesWith(sourceVertex);
		setMemorySize(tmpTask, baseMemory*1 / 9);
		
		//Inner iteration loop tasks -- START		
		JobTaskVertex updatesMatch = createTask(AsynchronousUpdateableMatchingOptimized.class, graph, dop, spi);
		updatesMatch.setVertexToShareInstancesWith(sourceVertex);
		setMemorySize(updatesMatch, baseMemory*8 /9);
		
//		JobTaskVertex countUpdates = createTask(CountUpdates.class, graph, dop, spi);
//		countUpdates.setVertexToShareInstancesWith(sourceVertex);
		//Inner iteration loop tasks -- END
		
		JobOutputVertex sinkVertex = createOutput(NullOutput.class, output, graph, dop, spi);
		sinkVertex.setVertexToShareInstancesWith(sourceVertex);
		
		//Connect tasks
		connectJobVertices(ShipStrategy.FORWARD, sourceVertex, convert, null, null);
		connectJobVertices(ShipStrategy.PARTITION_HASH, convert, initialState, null, null);
		
		connectJobVertices(ShipStrategy.FORWARD, initialState, initialUpdateAssigner, null, null);
		connectJobVertices(ShipStrategy.FORWARD, initialUpdateAssigner, tmpTask, null, null);
		
		
		NepheleUtil.connectAsyncBoundedRoundsIterationLoop(tmpTask, sinkVertex, null, 
				null, updatesMatch, ShipStrategy.PARTITION_HASH, 14, graph);
//		connectFixedPointIterationLoop(tmpTask, sinkVertex, new JobTaskVertex[] {distributeUpdates,
//				countUpdates}, 
//				distributeUpdates, countUpdates, updatesMatch, 
//				ShipStrategy.PARTITION_HASH, 
//				EmptyTerminationDecider.class, graph);
		
		connectJobVertices(ShipStrategy.FORWARD, initialState, updatesMatch, null, null);
		
		//Submit job
		submit(graph, getConfiguration());
	}
}
