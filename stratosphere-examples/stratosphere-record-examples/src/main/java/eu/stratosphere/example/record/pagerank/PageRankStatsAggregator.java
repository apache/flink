package eu.stratosphere.example.record.pagerank;

import eu.stratosphere.api.functions.aggregators.Aggregator;

public class PageRankStatsAggregator implements Aggregator<PageRankStats> {

	private double diff = 0;

	private double rank = 0;

	private double danglingRank = 0;

	private long numDanglingVertices = 0;

	private long numVertices = 0;

	private long edges = 0;

	@Override
	public PageRankStats getAggregate() {
		return new PageRankStats(diff, rank, danglingRank, numDanglingVertices, numVertices, edges);
	}

	public void aggregate(double diffDelta, double rankDelta, double danglingRankDelta, long danglingVerticesDelta,
			long verticesDelta, long edgesDelta) {
		diff += diffDelta;
		rank += rankDelta;
		danglingRank += danglingRankDelta;
		numDanglingVertices += danglingVerticesDelta;
		numVertices += verticesDelta;
		edges += edgesDelta;
	}

	@Override
	public void aggregate(PageRankStats pageRankStats) {
		diff += pageRankStats.diff();
		rank += pageRankStats.rank();
		danglingRank += pageRankStats.danglingRank();
		numDanglingVertices += pageRankStats.numDanglingVertices();
		numVertices += pageRankStats.numVertices();
		edges += pageRankStats.edges();
	}

	@Override
	public void reset() {
		diff = 0;
		rank = 0;
		danglingRank = 0;
		numDanglingVertices = 0;
		numVertices = 0;
		edges = 0;
	}
}
