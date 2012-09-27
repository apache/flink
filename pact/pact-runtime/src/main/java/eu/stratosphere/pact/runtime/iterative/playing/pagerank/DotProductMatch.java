package eu.stratosphere.pact.runtime.iterative.playing.pagerank;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;

public class DotProductMatch extends MatchStub {

  @Override
  public void match(PactRecord pageWithRank, PactRecord transitionMatrixEntry, Collector<PactRecord> collector)
      throws Exception {

    double rank = pageWithRank.getField(1, PactDouble.class).getValue();
    double transitionProbability = transitionMatrixEntry.getField(2, PactDouble.class).getValue();

    PactRecord result = pageWithRank;
    result.setField(1, new PactDouble(rank * transitionProbability));

    //long source = transitionMatrixEntry.getField(0, PactLong.class).getValue();
    //System.out.println("Match from " + source + " to " + vertexID + ": " + rank + " * " + transitionProbability + " = " +  (rank * transitionProbability));

    collector.collect(result);
  }
}
