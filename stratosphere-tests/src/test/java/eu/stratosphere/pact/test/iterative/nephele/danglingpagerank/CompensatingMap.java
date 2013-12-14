package eu.stratosphere.pact.test.iterative.nephele.danglingpagerank;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.test.iterative.nephele.ConfigUtils;
import eu.stratosphere.types.PactDouble;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.util.Collector;


import java.util.Set;

public class CompensatingMap extends MapStub {

  private int workerIndex;
  private int currentIteration;

  private long numVertices;

  private int failingIteration;
  private Set<Integer> failingWorkers;

  private double uniformRank;
  private double rescaleFactor;

  private PactDouble rank = new PactDouble();

  @Override
  public void open(Configuration parameters) throws Exception {

    workerIndex = getRuntimeContext().getIndexOfThisSubtask();
    currentIteration = getIterationRuntimeContext().getSuperstepNumber();
    failingIteration = ConfigUtils.asInteger("compensation.failingIteration", parameters);
    failingWorkers = ConfigUtils.asIntSet("compensation.failingWorker", parameters);
    numVertices = ConfigUtils.asLong("pageRank.numVertices", parameters);

    if (currentIteration > 1) {
    	PageRankStats stats = (PageRankStats) getIterationRuntimeContext().getPreviousIterationAggregate(CompensatableDotProductCoGroup.AGGREGATOR_NAME);

      uniformRank = 1d / (double) numVertices;
      double lostMassFactor = (numVertices - stats.numVertices()) / (double) numVertices;
      rescaleFactor = (1 - lostMassFactor) / stats.rank();
    }
  }

  @Override
  public void map(PactRecord pageWithRank, Collector<PactRecord> out) throws Exception {

    if (currentIteration == failingIteration + 1) {

      rank = pageWithRank.getField(1, rank);

      if (failingWorkers.contains(workerIndex)) {
         rank.setValue(uniformRank);
       } else {
        rank.setValue(rank.getValue() * rescaleFactor);
       }
      pageWithRank.setField(1, rank);
    }

    out.collect(pageWithRank);
  }

}
