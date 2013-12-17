package eu.stratosphere.example.record.incremental.pagerank;

import eu.stratosphere.util.Collector;
import eu.stratosphere.api.record.functions.JoinFunction;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.types.PactDouble;
import eu.stratosphere.types.PactLong;

public class PRDependenciesComputationMatchDelta extends JoinFunction {

	private final PactRecord result = new PactRecord();
	private final PactDouble partRank = new PactDouble();
	
	/*
	 * 
	 * (srcId, trgId, weight) x (vId, rank) => (trgId, rank / weight)
	 * 
	 */
	@Override
	public void match(PactRecord vertexWithRank, PactRecord edgeWithWeight, Collector<PactRecord> out) throws Exception {
		
		result.setField(0, edgeWithWeight.getField(1, PactLong.class));
		final long outLinks = edgeWithWeight.getField(2, PactLong.class).getValue();
		final double rank = vertexWithRank.getField(1, PactDouble.class).getValue();
		partRank.setValue(rank / (double) outLinks);
		result.setField(1, partRank);
		
		out.collect(result);
	}
}
