package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank;

import eu.stratosphere.pact.runtime.iterative.aggregate.Aggregator;

public class PageRankStatsAggregator implements Aggregator<PageRankStats> {

  private double diff = 0;
  private double rank = 0;
  private double danglingRank = 0;
  private long numDanglingVertices = 0;
  private long numVertices = 0;
  private long edges = 0;
  private double summedRank = 0;

  @Override
  public PageRankStats getAggregate() {
    return new PageRankStats(diff, rank, danglingRank, numDanglingVertices, numVertices, edges, summedRank);
  }

  public void aggregate(double diffDelta, double rankDelta, double danglingRankDelta, long danglingVerticesDelta,
      long verticesDelta, long edgesDelta, double summedRankDelta) {
    diff += diffDelta;
    rank += rankDelta;
    danglingRank += danglingRankDelta;
    numDanglingVertices += danglingVerticesDelta;
    numVertices += verticesDelta;
    edges += edgesDelta;
    summedRank += summedRankDelta;
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
  }
}
