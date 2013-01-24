package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.runtime.iterative.compensatable.ConfigUtils;
import eu.stratosphere.pact.runtime.iterative.concurrent.IterationContext;

import java.util.Iterator;
import java.util.Set;

public class CompensatableDotProductCoGroup extends CoGroupStub {

  private PactRecord accumulator;

  private int workerIndex;
  private int currentIteration;

  private int failingIteration;
  private Set<Integer> failingWorkers;

  private PageRankStatsAggregator aggregator =
      (PageRankStatsAggregator) new DiffL1NormConvergenceCriterion().createAggregator();

  private long numVertices;
  private long numDanglingVertices;
  private double dampingFactor;

  private double danglingRankFactor;

  private static final double BETA = 0.85;

  @Override
  public void open(Configuration parameters) throws Exception {
    accumulator = new PactRecord();

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
      PageRankStats previousAggregate = (PageRankStats) IterationContext.instance().getGlobalAggregate(workerIndex);
      danglingRankFactor = BETA * previousAggregate.danglingRank() / (double) numVertices;
    }

  }

  @Override
  public void coGroup(Iterator<PactRecord> currentPageRankIterator, Iterator<PactRecord> partialRanks,
      Collector<PactRecord> collector) {

    if (!currentPageRankIterator.hasNext()) {
      throw new IllegalStateException("No current page rank!");
    }
    PactRecord currentPageRank = currentPageRankIterator.next();

    long edges = 0;
    double summedRank = 0;
    while (partialRanks.hasNext()) {
      summedRank += partialRanks.next().getField(1, PactDouble.class).getValue();
      edges++;
    }

    double rank = BETA * summedRank + dampingFactor + danglingRankFactor;

    double currentRank = currentPageRank.getField(1, PactDouble.class).getValue();
    boolean isDangling = currentPageRank.getField(2, BooleanValue.class).get();

    double danglingRankToAggregate = isDangling ? rank : 0;
    long danglingVerticesToAggregate = isDangling ? 1 : 0;

    double diff = Math.abs(currentRank - rank);

    aggregator.aggregate(diff, rank, danglingRankToAggregate, danglingVerticesToAggregate, 1, edges, summedRank);

    accumulator.setField(0, currentPageRank.getField(0, PactLong.class));
    accumulator.setField(1, new PactDouble(rank));
    accumulator.setField(2, new BooleanValue(isDangling));

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
