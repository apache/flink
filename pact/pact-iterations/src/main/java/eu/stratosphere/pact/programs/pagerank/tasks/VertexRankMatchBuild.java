package eu.stratosphere.pact.programs.pagerank.tasks;

import java.util.HashMap;
import java.util.List;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.programs.pagerank.types.VertexNeighbourPartial;
import eu.stratosphere.pact.programs.pagerank.types.VertexNeighbourPartialAccessor;
import eu.stratosphere.pact.programs.pagerank.types.VertexPageRank;
import eu.stratosphere.pact.programs.pagerank.types.VertexPageRankAccessor;
import eu.stratosphere.pact.runtime.iterative.MutableHashTable;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;
import eu.stratosphere.pact.runtime.plugable.TypeComparator;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;

public class VertexRankMatchBuild extends AbstractIterativeTask {

	public static HashMap<Integer, MutableHashTable<Value, VertexNeighbourPartial>> tables =
			new  HashMap<Integer, MutableHashTable<Value,VertexNeighbourPartial>>();
	
	private static final int MATCH_CHUNCK_SIZE = 1024*1024;
	
	private boolean firstRun  = true;

	private List<MemorySegment> joinMem;
	
	private int iteration = 0;
	
	@Override
	public void runIteration(IterationIterator iterationIter) throws Exception {
		if(firstRun) {
			int chunckSize = MATCH_CHUNCK_SIZE;
			
			joinMem = 
				memoryManager.allocateStrict(this, (int) (memorySize/chunckSize), chunckSize);
			firstRun = false;
		}
		
		TypeAccessorsV2 buildAccess = new VertexPageRankAccessor();
		TypeAccessorsV2 probeAccess = new VertexNeighbourPartialAccessor();
		TypeComparator comp = new MatchComparator();
		
		if(iteration > 0) {
			tables.get(iteration - 1).close();
		}
		
		MutableHashTable<Value, VertexNeighbourPartial> table = 
				new MutableHashTable<Value, VertexNeighbourPartial>(buildAccess, probeAccess, comp, 
				joinMem, ioManager, 128);
		table.open(iterationIter, EmptyMutableObjectIterator.<VertexNeighbourPartial>get());
		
		tables.put(iteration, table);
		
		iteration++;
	}
	
	@Override
	protected void initTask() {
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	public static final class MatchComparator implements TypeComparator<VertexNeighbourPartial, 
	VertexPageRank>
	{
		private long key;
	
		@Override
		public void setReference(VertexNeighbourPartial reference, 
				TypeAccessorsV2<VertexNeighbourPartial> accessor) {
			this.key = reference.getVid();
		}
	
		@Override
		public boolean equalToReference(VertexPageRank candidate, TypeAccessorsV2<VertexPageRank> accessor) {
			return this.key == candidate.getVid();
		}
	}
}
