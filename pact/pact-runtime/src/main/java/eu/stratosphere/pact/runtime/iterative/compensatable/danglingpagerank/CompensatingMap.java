package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.runtime.iterative.compensatable.ConfigUtils;
import eu.stratosphere.pact.runtime.iterative.concurrent.IterationContext;

import java.util.Set;

public class CompensatingMap extends MapStub {

  private int workerIndex;
  private int currentIteration;

  private long numVertices;

  private int failingIteration;
  private Set<Integer> failingWorkers;

  private double uniformRank;
  private double rescaleFactor;

  @Override
  public void open(Configuration parameters) throws Exception {

    workerIndex = ConfigUtils.asInteger("pact.parallel.task.id", parameters);
    currentIteration = ConfigUtils.asInteger("pact.iterations.currentIteration", parameters);
    failingIteration = ConfigUtils.asInteger("compensation.failingIteration", parameters);
    failingWorkers = ConfigUtils.asIntSet("compensation.failingWorker", parameters);
    numVertices = ConfigUtils.asLong("pageRank.numVertices", parameters);

    if (currentIteration > 1) {
      PageRankStats stats = (PageRankStats) IterationContext.instance().getGlobalAggregate(workerIndex);

      uniformRank = 1d / (double) numVertices;
      double lostMassFactor = (numVertices - stats.numVertices()) / (double) numVertices;
      rescaleFactor = (1 - lostMassFactor) / stats.rank();
    }
  }

  @Override
  public void map(PactRecord pageWithRank, Collector<PactRecord> out) throws Exception {

    if (currentIteration == failingIteration + 1) {

      double rank = pageWithRank.getField(1, PactDouble.class).getValue();

      if (failingWorkers.contains(workerIndex)) {
         rank = uniformRank;
       } else {
        rank *= rescaleFactor;
       }
      pageWithRank.setField(1, new PactDouble(rank));
    }

    out.collect(pageWithRank);
  }

}
