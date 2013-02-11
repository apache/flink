package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.custom;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.generic.stub.AbstractStub;
import eu.stratosphere.pact.generic.stub.GenericCoGrouper;
import eu.stratosphere.pact.runtime.iterative.compensatable.ConfigUtils;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.DiffL1NormConvergenceCriterion;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.PageRankStats;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.PageRankStatsAggregator;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.custom.types.VertexWithRank;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.custom.types.VertexWithRankAndDangling;
import eu.stratosphere.pact.runtime.iterative.concurrent.IterationContext;

import java.util.Iterator;
import java.util.Set;

public class CustomCompensatableDotProductCoGroup extends AbstractStub implements GenericCoGrouper<VertexWithRankAndDangling, VertexWithRank, VertexWithRankAndDangling> {

	private VertexWithRankAndDangling accumulator = new VertexWithRankAndDangling();

	private PageRankStatsAggregator aggregator =
		(PageRankStatsAggregator) new DiffL1NormConvergenceCriterion().createAggregator();

	private long numVertices;

	private long numDanglingVertices;

	private double dampingFactor;

	private double danglingRankFactor;

	private static final double BETA = 0.85;
	
	private int workerIndex;
	
	private int currentIteration;
	
	private int failingIteration;
	
	private Set<Integer> failingWorkers;

	@Override
	public void open(Configuration parameters) throws Exception {
		workerIndex = ConfigUtils.asInteger("pact.parallel.task.id", parameters);
		currentIteration = ConfigUtils.asInteger("pact.iterations.currentIteration", parameters);
		failingIteration = ConfigUtils.asInteger("compensation.failingIteration", parameters);
		failingWorkers = ConfigUtils.asIntSet("compensation.failingWorker", parameters);
		
		numVertices = ConfigUtils.asLong("pageRank.numVertices", parameters);
		numDanglingVertices = ConfigUtils.asLong("pageRank.numDanglingVertices", parameters);

		aggregator.reset();

		dampingFactor = (1d - BETA) / (double) numVertices;

		if (currentIteration == 1) {
			danglingRankFactor = BETA * (double) numDanglingVertices / ((double) numVertices * (double) numVertices);
		} else {
			PageRankStats previousAggregate = (PageRankStats) IterationContext.instance().getGlobalAggregate(
				workerIndex);
			danglingRankFactor = BETA * previousAggregate.danglingRank() / (double) numVertices;
		}
	}

	@Override
	public void coGroup(Iterator<VertexWithRankAndDangling> currentPageRankIterator, Iterator<VertexWithRank> partialRanks,
			Collector<VertexWithRankAndDangling> collector)
	{
		if (!currentPageRankIterator.hasNext()) {
			long missingVertex = partialRanks.next().getVertexID();
			throw new IllegalStateException("No current page rank for vertex [" + missingVertex + "]!");
		}

		VertexWithRankAndDangling currentPageRank = currentPageRankIterator.next();

		long edges = 0;
		double summedRank = 0;
		while (partialRanks.hasNext()) {
			summedRank += partialRanks.next().getRank();
			edges++;
		}

		double rank = BETA * summedRank + dampingFactor + danglingRankFactor;

		double currentRank = currentPageRank.getRank();
		boolean isDangling = currentPageRank.isDangling();

		double danglingRankToAggregate = isDangling ? rank : 0;
		long danglingVerticesToAggregate = isDangling ? 1 : 0;

		double diff = Math.abs(currentRank - rank);

		aggregator.aggregate(diff, rank, danglingRankToAggregate, danglingVerticesToAggregate, 1, edges, summedRank, 0);

		accumulator.setVertexID(currentPageRank.getVertexID());
		accumulator.setRank(rank);
		accumulator.setDangling(isDangling);

		collector.collect(accumulator);
	}

	@Override
	public void close() throws Exception {
		if (currentIteration == failingIteration && failingWorkers.contains(workerIndex)) {
			aggregator.reset();
		}
		IterationContext.instance().setAggregate(workerIndex, aggregator.getAggregate());
	}
}
