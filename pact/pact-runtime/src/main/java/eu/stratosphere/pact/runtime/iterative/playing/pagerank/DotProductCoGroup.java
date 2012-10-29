package eu.stratosphere.pact.runtime.iterative.playing.pagerank;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.runtime.iterative.aggregate.Aggregator;
import eu.stratosphere.pact.runtime.iterative.concurrent.IterationContext;

import java.util.Iterator;

public class DotProductCoGroup extends CoGroupStub {

  private PactRecord accumulator;
  private double diffOfPartition;
  private int workerIndex;
  private Aggregator<PactDouble> aggregator = new L1NormConvergenceCriterion().createAggregator();;


  @Override
  public void open(Configuration parameters) throws Exception {
    accumulator = new PactRecord();
    workerIndex = parameters.getInteger("pact.parallel.task.id", -1);
    if (workerIndex == -1) {
      throw new IllegalStateException("Invalid workerIndex " + workerIndex);
    }
    diffOfPartition = 0;

    aggregator.reset();
  }

  @Override
  public void coGroup(Iterator<PactRecord> currentPageRankIterator, Iterator<PactRecord> partialRanks,
      Collector<PactRecord> collector) {

    //PactRecord first = partialRanks.next();
    //long partialVertex = first.getField(0, PactLong.class).getValue();

    if (!currentPageRankIterator.hasNext()) {
      throw new IllegalStateException("No current ");
    }
    PactRecord currentPageRank = currentPageRankIterator.next();

    double rank = 0;

    while (partialRanks.hasNext()) {
      PactRecord record = partialRanks.next();
      if (currentPageRank.getField(0, PactLong.class).getValue() != record.getField(0, PactLong.class).getValue()) {
        throw new IllegalStateException();
      }
      rank += record.getField(1, PactDouble.class).getValue();
    }

    double currentRank = currentPageRank.getField(1, PactDouble.class).getValue();

    double diff = Math.abs(currentRank - rank);

    diffOfPartition += diff;

//    System.out.println("RANK OF " + currentPageRank.getField(0, PactLong.class).getValue() + ": " + currentRank + " --> " + rank +  "(" + diff + ")");

    accumulator.setField(0, currentPageRank.getField(0, PactLong.class));
    accumulator.setField(1, new PactDouble(rank));

//    buffer.append("= " + accumulator.getField(0, PactLong.class) + " " + rank + ")))");
//    System.out.println(buffer);

    collector.collect(accumulator);
  }

  @Override
  public void close() throws Exception {
    //witzlos
    aggregator.aggregate(new PactDouble(diffOfPartition));
    IterationContext.instance().setAggregate(workerIndex, aggregator.getAggregate());
  }
}
