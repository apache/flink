package eu.stratosphere.test.iterative.nephele.danglingpagerank;

import eu.stratosphere.api.functions.aggregators.Aggregator;

public class PageRankStatsAggregator implements Aggregator<PageRankStats> {

	private double diff = 0;

	private double rank = 0;

	private double danglingRank = 0;

	private long numDanglingVertices = 0;

	private long numVertices = 0;

	private long edges = 0;

	private double summedRank = 0;

	private double finalDiff = 0;

	@Override
	public PageRankStats getAggregate() {
		return new PageRankStats(diff, rank, danglingRank, numDanglingVertices, numVertices, edges, summedRank,
			finalDiff);
	}

	public void aggregate(double diffDelta, double rankDelta, double danglingRankDelta, long danglingVerticesDelta,
			long verticesDelta, long edgesDelta, double summedRankDelta, double finalDiffDelta) {
		diff += diffDelta;
		rank += rankDelta;
		danglingRank += danglingRankDelta;
		numDanglingVertices += danglingVerticesDelta;
		numVertices += verticesDelta;
		edges += edgesDelta;
		summedRank += summedRankDelta;
		finalDiff += finalDiffDelta;
	}

	@Override
	public void aggregate(PageRankStats pageRankStats) {
		diff += pageRankStats.diff();
		rank += pageRankStats.rank();
		danglingRank += pageRankStats.danglingRank();
		numDanglingVertices += pageRankStats.numDanglingVertices();
		numVertices += pageRankStats.numVertices();
		edges += pageRankStats.edges();
		summedRank += pageRankStats.summedRank();
		finalDiff += pageRankStats.finalDiff();
	}

	@Override
	public void reset() {
		diff = 0;
		rank = 0;
		danglingRank = 0;
		numDanglingVertices = 0;
		numVertices = 0;
		edges = 0;
		summedRank = 0;
		finalDiff = 0;
	}
}
