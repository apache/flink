package eu.stratosphere.pact.runtime.iterative.compensatable;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.runtime.iterative.concurrent.IterationContext;

public class CompensatingMap extends MapStub {

  private int workerIndex;
  private int currentIteration;

  private PageRankStats stats;
  private long numVertices;

  private double compensatedRank;

  private int failingIteration;
  private int failingWorker;
  private double messageLoss;

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
      System.out.println("CompensatingMap " + workerIndex + " " + currentIteration + "\n" + stats);
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

    if (currentIteration == failingIteration + 1 && workerIndex == failingWorker) {
      compensatedRank = (1d - stats.rank()) / (double) (numVertices - stats.numVertices());
    }

  }

  @Override
  public void map(PactRecord pageWithRank, Collector<PactRecord> out) throws Exception {

    if (currentIteration == failingIteration + 1 && workerIndex == failingWorker) {
      pageWithRank.setField(1, new PactDouble(compensatedRank));
    }

    //long vertex = pageWithRank.getField(0, PactLong.class).getValue();
    //System.out.println("Compensating value of vertex " + vertex + " with rank " + compensatedRank);

    out.collect(pageWithRank);
  }
}
