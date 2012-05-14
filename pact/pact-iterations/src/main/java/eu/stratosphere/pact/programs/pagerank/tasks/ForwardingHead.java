package eu.stratosphere.pact.programs.pagerank.tasks;

import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationHead;
import eu.stratosphere.pact.iterative.nephele.util.OutputCollectorV2;
import eu.stratosphere.pact.programs.pagerank.types.VertexPageRank;

public class ForwardingHead extends IterationHead {

	@Override
	public void finish(MutableObjectIterator<Value> iter,
			OutputCollectorV2 output) throws Exception {
		//TODO
	}

	@Override
	public void processInput(MutableObjectIterator<Value> iter,
			OutputCollectorV2 output) throws Exception {
		VertexPageRank pRank = new VertexPageRank();
		
		while(iter.next(pRank)) {
			output.collect(pRank);
		}
	}

	@Override
	public void processUpdates(MutableObjectIterator<Value> iter,
			OutputCollectorV2 output) throws Exception {
		VertexPageRank pRank = new VertexPageRank();
		
		while(iter.next(pRank)) {
			output.collect(pRank);
		}
	}

}
