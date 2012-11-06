package eu.stratosphere.pact.runtime.iterative.compensatable.pagerank;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.runtime.iterative.concurrent.IterationContext;

public class NormalizingMap extends MapStub {

  private int workerIndex;
  private int currentIteration;

  private PageRankStats stats;
  private long numVertices;

  private int failingIteration;
  private int failingWorker;
  private double messageLoss;

  private double uniformRank;
  private double rescaleFactor;

  @Override
  public void open(Configuration parameters) throws Exception {
    workerIndex = parameters.getInteger("pact.parallel.task.id", -1);
    if (workerIndex == -1) {
      throw new IllegalStateException("Invalid workerIndex " + workerIndex);
    }
    currentIteration = parameters.getInteger("pact.iterations.currentIteration", -1);
    if (currentIteration == -1) {
      throw new IllegalStateException();
    }
    if (currentIteration > 1) {
      stats = (PageRankStats) IterationContext.instance().getGlobalAggregate(workerIndex);
      //System.out.println("NormalizingMap " + workerIndex + " " + currentIteration + "\n" + stats);
    }

    failingIteration = parameters.getInteger("compensation.failingIteration", -1);
    if (failingIteration == -1) {
      throw new IllegalStateException();
    }
    failingWorker = parameters.getInteger("compensation.failingWorker", -1);
    if (failingWorker == -1) {
      throw new IllegalStateException();
    }
    messageLoss = Double.parseDouble(parameters.getString("compensation.messageLoss", "-1"));
    if (messageLoss == -1) {
      throw new IllegalStateException();
    }

    numVertices = parameters.getLong("pageRank.numVertices", -1);
    if (numVertices == -1) {
      throw new IllegalStateException();
    }

    if (currentIteration > 1) {
      uniformRank = 1d / (double) numVertices;
      double lostMassFactor = (numVertices - stats.numVertices()) / (double) numVertices;
      rescaleFactor = (1 - lostMassFactor) / stats.rank();
    }
  }

  @Override
  public void map(PactRecord pageWithRank, Collector<PactRecord> out) throws Exception {

    if (currentIteration > 1) {

      double rank = pageWithRank.getField(1, PactDouble.class).getValue();

      if (currentIteration != failingIteration + 1) {
        /* normalize */
        rank /= stats.rank();
      } else {
           if (workerIndex == failingWorker) {
             rank = uniformRank;
           } else {
            rank *= rescaleFactor;
           }
      }
      pageWithRank.setField(1, new PactDouble(rank));
    }

    out.collect(pageWithRank);
  }

}
