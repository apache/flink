package eu.stratosphere.pact.runtime.iterative.compensatable.pagerank;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.runtime.iterative.compensatable.ConfigUtils;
import eu.stratosphere.pact.runtime.iterative.concurrent.IterationContext;

import java.util.Iterator;

public class CompensatableDotProductCoGroup extends CoGroupStub {

  private PactRecord accumulator;

  private int workerIndex;
  private int currentIteration;

  private int failingIteration;
  private int failingWorker;

  private PageRankStatsAggregator aggregator =
      (PageRankStatsAggregator) new DiffL1NormConvergenceCriterion().createAggregator();

  private long numVertices;
  private double dampingFactor;

  private static final double BETA = 0.85;

  @Override
  public void open(Configuration parameters) throws Exception {
    accumulator = new PactRecord();

    workerIndex = ConfigUtils.asInteger("pact.parallel.task.id", parameters);
    currentIteration = ConfigUtils.asInteger("pact.iterations.currentIteration", parameters);
    failingIteration = ConfigUtils.asInteger("compensation.failingIteration", parameters);
    failingWorker = ConfigUtils.asInteger("compensation.failingWorker", parameters);
    numVertices = ConfigUtils.asLong("pageRank.numVertices", parameters);

    aggregator.reset();

    dampingFactor = (1d - BETA) * (1d / (double) numVertices);
  }

  @Override
  public void coGroup(Iterator<PactRecord> currentPageRankIterator, Iterator<PactRecord> partialRanks,
      Collector<PactRecord> collector) {

    if (!currentPageRankIterator.hasNext()) {
      throw new IllegalStateException("No current page rank!");
    }
    PactRecord currentPageRank = currentPageRankIterator.next();

    double rank = 0;
    while (partialRanks.hasNext()) {
      rank += partialRanks.next().getField(1, PactDouble.class).getValue();
    }

    rank = BETA * rank + dampingFactor;

    double currentRank = currentPageRank.getField(1, PactDouble.class).getValue();

    double diff = Math.abs(currentRank - rank);

    aggregator.aggregate(diff, rank, 1);

    accumulator.setField(0, currentPageRank.getField(0, PactLong.class));
    accumulator.setField(1, new PactDouble(rank));

    collector.collect(accumulator);
  }

  @Override
  public void close() throws Exception {
    if (currentIteration == failingIteration && workerIndex == failingWorker) {
      aggregator.reset();
    }
    IterationContext.instance().setAggregate(workerIndex, aggregator.getAggregate());
  }
}
