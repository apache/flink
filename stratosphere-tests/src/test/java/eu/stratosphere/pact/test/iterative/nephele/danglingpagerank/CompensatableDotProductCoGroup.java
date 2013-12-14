package eu.stratosphere.pact.test.iterative.nephele.danglingpagerank;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.test.iterative.nephele.ConfigUtils;
import eu.stratosphere.types.PactDouble;
import eu.stratosphere.types.PactLong;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.util.Collector;

import java.util.Iterator;
import java.util.Set;

public class CompensatableDotProductCoGroup extends CoGroupStub {
	
	public static final String AGGREGATOR_NAME = "pagerank.aggregator";

	private PactRecord accumulator = new PactRecord();

	private int workerIndex;

	private int currentIteration;

	private int failingIteration;

	private Set<Integer> failingWorkers;

	private PageRankStatsAggregator aggregator;

	private long numVertices;

	private long numDanglingVertices;

	private double dampingFactor;

	private double danglingRankFactor;

	private static final double BETA = 0.85;

	private final PactDouble newRank = new PactDouble();

	private BooleanValue isDangling = new BooleanValue();

	private PactLong vertexID = new PactLong();

	private PactDouble doubleInstance = new PactDouble();

	@Override
	public void open(Configuration parameters) throws Exception {
		workerIndex = getRuntimeContext().getIndexOfThisSubtask();
		currentIteration = getIterationRuntimeContext().getSuperstepNumber();
		
		failingIteration = ConfigUtils.asInteger("compensation.failingIteration", parameters);
		failingWorkers = ConfigUtils.asIntSet("compensation.failingWorker", parameters);
		numVertices = ConfigUtils.asLong("pageRank.numVertices", parameters);
		numDanglingVertices = ConfigUtils.asLong("pageRank.numDanglingVertices", parameters);

		dampingFactor = (1d - BETA) / (double) numVertices;
		
		aggregator = (PageRankStatsAggregator) getIterationRuntimeContext().<PageRankStats>getIterationAggregator(AGGREGATOR_NAME);
		
		if (currentIteration == 1) {
			danglingRankFactor = BETA * (double) numDanglingVertices / ((double) numVertices * (double) numVertices);
		} else {
			PageRankStats previousAggregate = getIterationRuntimeContext().getPreviousIterationAggregate(AGGREGATOR_NAME);
			danglingRankFactor = BETA * previousAggregate.danglingRank() / (double) numVertices;
		}
	}

	@Override
	public void coGroup(Iterator<PactRecord> currentPageRankIterator, Iterator<PactRecord> partialRanks,
			Collector<PactRecord> collector) {

		if (!currentPageRankIterator.hasNext()) {
			long missingVertex = partialRanks.next().getField(0, PactLong.class).getValue();
			throw new IllegalStateException("No current page rank for vertex [" + missingVertex + "]!");
		}

		PactRecord currentPageRank = currentPageRankIterator.next();

		long edges = 0;
		double summedRank = 0;
		while (partialRanks.hasNext()) {
			summedRank += partialRanks.next().getField(1, doubleInstance).getValue();
			edges++;
		}

		double rank = BETA * summedRank + dampingFactor + danglingRankFactor;

		double currentRank = currentPageRank.getField(1, doubleInstance).getValue();
		isDangling = currentPageRank.getField(2, isDangling);

		double danglingRankToAggregate = isDangling.get() ? rank : 0;
		long danglingVerticesToAggregate = isDangling.get() ? 1 : 0;

		double diff = Math.abs(currentRank - rank);

		aggregator.aggregate(diff, rank, danglingRankToAggregate, danglingVerticesToAggregate, 1, edges, summedRank, 0);

		newRank.setValue(rank);

		accumulator.setField(0, currentPageRank.getField(0, vertexID));
		accumulator.setField(1, newRank);
		accumulator.setField(2, isDangling);

		collector.collect(accumulator);
	}

	@Override
	public void close() throws Exception {
		if (currentIteration == failingIteration && failingWorkers.contains(workerIndex)) {
			aggregator.reset();
		}
	}
}
