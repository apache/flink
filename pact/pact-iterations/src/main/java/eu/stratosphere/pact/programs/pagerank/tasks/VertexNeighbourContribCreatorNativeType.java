package eu.stratosphere.pact.programs.pagerank.tasks;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;
import eu.stratosphere.pact.programs.pagerank.types.VertexNeighbourPartial;
import eu.stratosphere.pact.programs.pagerank.types.VertexNeighbourPartialAccessor;
import eu.stratosphere.pact.programs.preparation.tasks.LongList;

public class VertexNeighbourContribCreatorNativeType extends AbstractMinimalTask {
	
	PactLong number = new PactLong();
	LongList adjList = new LongList();
	
	PactRecord result = new PactRecord();
	
	@Override
	protected void initTask() {
		outputAccessors[0] = new VertexNeighbourPartialAccessor();
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void run() throws Exception {
		VertexNeighbourPartial result = new VertexNeighbourPartial();
		PactRecord rec = new PactRecord();
		
		while(inputs[0].next(rec)) {
			long vid = rec.getField(0, number).getValue();
			
			adjList = rec.getField(1, adjList);
			long[] neighbours = adjList.getList();
			int numNeighbours = adjList.getLength();
			
			//Create self contribution
			result.setVid(vid);
			result.setNid(vid);
			result.setPartial(0);
			output.collect(result);
			
			//Create neighbour contribution
			double contrib = 1.0 / numNeighbours;
			for (int i = 0; i < numNeighbours; i++) {
				result.setNid(neighbours[i]);
				result.setPartial(contrib);
				output.collect(result);
			}
		}
	}

}
