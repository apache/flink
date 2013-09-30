package eu.stratosphere.pact.example.pagerank;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirst;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactBoolean;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.example.util.ConfigUtils;

import java.io.Serializable;
import java.util.Iterator;

/**
 * In schema is_
 * INPUT = (pageId, currentRank, dangling), (pageId, partialRank).
 * OUTPUT = (pageId, newRank, dangling)
 */
@ConstantFieldsFirst(0)
public class DotProductCoGroup extends CoGroupStub implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public static final String NUM_VERTICES_PARAMETER = "pageRank.numVertices";
	
	public static final String NUM_DANGLING_VERTICES_PARAMETER = "pageRank.numDanglingVertices";
	
	public static final String AGGREGATOR_NAME = "pagerank.aggregator";
	
	private static final double BETA = 0.85;

	
	private PageRankStatsAggregator aggregator;

	private long numVertices;

	private long numDanglingVertices;

	private double dampingFactor;

	private double danglingRankFactor;
	
	
	private PactRecord accumulator = new PactRecord();

	private final PactDouble newRank = new PactDouble();

	private PactBoolean isDangling = new PactBoolean();

	private PactLong vertexID = new PactLong();

	private PactDouble doubleInstance = new PactDouble();

	@Override
	public void open(Configuration parameters) throws Exception {
		int currentIteration = getIterationRuntimeContext().getSuperstepNumber();
		
		numVertices = ConfigUtils.asLong(NUM_VERTICES_PARAMETER, parameters);
		numDanglingVertices = ConfigUtils.asLong(NUM_DANGLING_VERTICES_PARAMETER, parameters);

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
			Collector<PactRecord> collector)
	{
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
		
		// maintain statistics to compensate for probability loss on dangling nodes
		double danglingRankToAggregate = isDangling.get() ? rank : 0;
		long danglingVerticesToAggregate = isDangling.get() ? 1 : 0;
		double diff = Math.abs(currentRank - rank);
		aggregator.aggregate(diff, rank, danglingRankToAggregate, danglingVerticesToAggregate, 1, edges);
		
		// return the new record
		newRank.setValue(rank);
		accumulator.setField(0, currentPageRank.getField(0, vertexID));
		accumulator.setField(1, newRank);
		accumulator.setField(2, isDangling);
		collector.collect(accumulator);
	}
}
