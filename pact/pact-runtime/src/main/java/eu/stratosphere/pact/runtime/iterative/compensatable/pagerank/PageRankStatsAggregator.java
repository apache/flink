package eu.stratosphere.pact.runtime.iterative.compensatable.pagerank;

import eu.stratosphere.pact.runtime.iterative.aggregate.Aggregator;

public class PageRankStatsAggregator implements Aggregator<PageRankStats> {

  private double diff = 0;
  private double rank = 0;
  private long numVertices = 0;

  @Override
  public PageRankStats getAggregate() {
    return new PageRankStats(diff, rank, numVertices);
  }

  public void aggregate(double diffDelta, double rankDelta, long verticesDelta) {
    diff += diffDelta;
    rank += rankDelta;
    numVertices += verticesDelta;
  }

  @Override
  public void aggregate(PageRankStats pageRankStats) {
    diff += pageRankStats.diff();
    rank += pageRankStats.rank();
    numVertices += pageRankStats.numVertices();
  }

  @Override
  public void reset() {
    diff = 0;
    rank = 0;
    numVertices = 0;
  }
}
