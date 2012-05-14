package eu.stratosphere.pact.programs.pagerank.tasks;

import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.programs.pagerank.types.VertexNeighbourPartial;
import eu.stratosphere.pact.programs.pagerank.types.VertexNeighbourPartialAccessor;
import eu.stratosphere.pact.programs.pagerank.types.VertexPageRank;
import eu.stratosphere.pact.runtime.iterative.MutableHashTable;
import eu.stratosphere.pact.runtime.iterative.MutableHashTable.HashBucketIterator;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;
import eu.stratosphere.pact.runtime.plugable.TypeComparator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableMutableObjectIteratorV2;

public class ParallelVertexRankMatchProbeCaching extends AbstractIterativeTask {

	boolean firstRound = true;
	private SpillingResettableMutableObjectIteratorV2<Value> stateIterator;
	
	private int iteration = 0;
	@Override
	public void runIteration(IterationIterator iterationIter) throws Exception {
		if(firstRound) {
			firstRound = false;
			
			stateIterator = new SpillingResettableMutableObjectIteratorV2<Value>(memoryManager, ioManager, 
					inputs[1], (TypeAccessorsV2<Value>) accessors[1], memorySize, this);
			stateIterator.open();
		}
		
		stateIterator.reset();
		
		VertexNeighbourPartial state = new VertexNeighbourPartial();
		VertexPageRank pageRank = new VertexPageRank();
		VertexPageRank result = new VertexPageRank();
		
		//Blocks until Hashtable is build
		while(iterationIter.next(pageRank));
		
		MutableHashTable<Value, VertexNeighbourPartial> table = 
				ParallelVertexRankMatchBuild.tables.get(iteration).duplicate(((TypeComparator)new VertexRankMatchBuild.MatchComparator()));
		
		while(stateIterator.next(state)) {
			HashBucketIterator<Value, VertexNeighbourPartial> tableIter = table.getMatchesFor(state);
			while(tableIter.next(pageRank)) {
				double rank = pageRank.getRank();
				double partial = state.getPartial();
				
				if(Double.isNaN(rank*partial)) {
					LOG.info("NAN: "  + pageRank.getVid() + "::" + rank + " // " + pageRank.getRank() +"::"+ state.getPartial() );
				} else {
					result.setVid(state.getNid());
					result.setRank(rank*partial);
					output.collect(result);
				}
			}
		}
		
		//table.close();
		iteration++;
	}

	@Override
	protected void initTask() {
		accessors[1] = new VertexNeighbourPartialAccessor();
	}

	@Override
	public int getNumberOfInputs() {
		return 2;
	}
}
