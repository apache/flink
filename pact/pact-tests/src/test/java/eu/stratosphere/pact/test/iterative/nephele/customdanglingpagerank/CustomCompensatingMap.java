package eu.stratosphere.pact.test.iterative.nephele.customdanglingpagerank;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.generic.stub.AbstractStub;
import eu.stratosphere.pact.generic.stub.GenericMapper;
import eu.stratosphere.pact.test.iterative.nephele.ConfigUtils;
import eu.stratosphere.pact.test.iterative.nephele.customdanglingpagerank.types.VertexWithRankAndDangling;
import eu.stratosphere.pact.test.iterative.nephele.danglingpagerank.PageRankStats;

import java.util.Set;

public class CustomCompensatingMap extends AbstractStub implements GenericMapper<VertexWithRankAndDangling, VertexWithRankAndDangling> {
	
	private boolean isFailureIteration;

	private boolean isFailingWorker;

	private double uniformRank;

	private double rescaleFactor;

	@Override
	public void open(Configuration parameters) throws Exception {
		int currentIteration = getIterationRuntimeContext().getSuperstepNumber();
		int failingIteration = ConfigUtils.asInteger("compensation.failingIteration", parameters);
		isFailureIteration = currentIteration == failingIteration + 1;
		
		int workerIndex = getRuntimeContext().getIndexOfThisSubtask();
		Set<Integer> failingWorkers = ConfigUtils.asIntSet("compensation.failingWorker", parameters);
		isFailingWorker = failingWorkers.contains(workerIndex);
		
		long numVertices = ConfigUtils.asLong("pageRank.numVertices", parameters);

		if (currentIteration > 1) {
			
			PageRankStats stats = (PageRankStats) getIterationRuntimeContext().getPreviousIterationAggregate(CustomCompensatableDotProductCoGroup.AGGREGATOR_NAME);

			uniformRank = 1d / (double) numVertices;
			double lostMassFactor = (numVertices - stats.numVertices()) / (double) numVertices;
			rescaleFactor = (1 - lostMassFactor) / stats.rank();
		}
	}

	@Override
	public void map(VertexWithRankAndDangling pageWithRank, Collector<VertexWithRankAndDangling> out) throws Exception {

		if (isFailureIteration) {
			double rank = pageWithRank.getRank();

			if (isFailingWorker) {
				pageWithRank.setRank(uniformRank);
			} else {
				pageWithRank.setRank(rank * rescaleFactor);
			}
		}
		out.collect(pageWithRank);
	}
}
