package eu.stratosphere.pact.programs.pagerank.tasks;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.programs.pagerank.types.VertexPageRank;
import eu.stratosphere.pact.programs.pagerank.types.VertexPageRankAccessor;

public class InitialRankAssigner extends AbstractMinimalTask {
  VertexPageRank vRank = new VertexPageRank();
  PactLong number = new PactLong();

  @Override
  protected void initTask() {
    outputAccessors[0] = new VertexPageRankAccessor();
  }

  @Override
  public int getNumberOfInputs() {
    return 1;
  }

  @Override
  public void run() throws Exception {
    PactRecord rec = new PactRecord();
    double initialRank = 1.0 / 14052;

    while (inputs[0].next(rec)) {
      vRank.setVid(rec.getField(0, number).getValue());
      vRank.setRank(initialRank);
      output.collect(vRank);
    }
  }

}
