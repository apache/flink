package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank;

import eu.stratosphere.pact.runtime.iterative.aggregate.Aggregator;

public class PageRankStatsAggregator implements Aggregator<PageRankStats> {

  private double diff = 0;
  private double rank = 0;
  private double danglingRank = 0;
  private long numVertices = 0;

  @Override
  public PageRankStats getAggregate() {
    return new PageRankStats(diff, rank, danglingRank, numVertices);
  }

  public void aggregate(double diffDelta, double rankDelta, double danglingRankDelta, long verticesDelta) {
    diff += diffDelta;
    rank += rankDelta;
    danglingRank += danglingRankDelta;
    numVertices += verticesDelta;
  }

  @Override
  public void aggregate(PageRankStats pageRankStats) {
    diff += pageRankStats.diff();
    rank += pageRankStats.rank();
    danglingRank += pageRankStats.danglingRank();
    numVertices += pageRankStats.numVertices();
  }

  @Override
  public void reset() {
    diff = 0;
    rank = 0;
    danglingRank = 0;
    numVertices = 0;
  }
}
