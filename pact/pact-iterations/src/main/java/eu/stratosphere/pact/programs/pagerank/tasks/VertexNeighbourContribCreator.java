package eu.stratosphere.pact.programs.pagerank.tasks;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;
import eu.stratosphere.pact.programs.connected.types.LongPactRecordAccessor;
import eu.stratosphere.pact.programs.preparation.tasks.LongList;

public class VertexNeighbourContribCreator extends AbstractMinimalTask {
	
	PactLong vid = new PactLong();
	LongList adjList = new LongList();
	
	PactRecord result = new PactRecord();
	PactLong nid = new PactLong();
	PactDouble contrib = new PactDouble();
	
	@SuppressWarnings("unchecked")
	@Override
	protected void initTask() {
		outputAccessors[0] = new LongPactRecordAccessor(new int[] {1}, 
				new Class[] {PactLong.class});
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void run() throws Exception {
		PactRecord rec = new PactRecord();
		
		while(inputs[0].next(rec)) {
			vid = rec.getField(0, vid);
			
			adjList = rec.getField(1, adjList);
			long[] neighbours = adjList.getList();
			int numNeighbours = adjList.getLength();
			
			//Create self contribution
			contrib.setValue(0);
			result.setField(0, vid);
			result.setField(1, vid);
			result.setField(2, contrib);
			output.collect(result);
			
			//Create neighbour contribution
			contrib.setValue(1d / numNeighbours);
			for (int i = 0; i < numNeighbours; i++) {
				nid.setValue(neighbours[i]);
				result.setField(1, nid);
				result.setField(2, contrib);
				output.collect(result);
			}
		}
	}

}
