package eu.stratosphere.pact.runtime.iterative.playing.connectedcomponents;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;


public class UpdateCompontentIDMatch extends MatchStub {

  @Override
  public void match(PactRecord newVertexWithComponent, PactRecord currentVertexWithComponent,
      Collector<PactRecord> out) throws Exception {

    long candidateComponentID = newVertexWithComponent.getField(1, PactLong.class).getValue();
    long currentComponentID = currentVertexWithComponent.getField(1, PactLong.class).getValue();

    if (candidateComponentID < currentComponentID) {
      out.collect(newVertexWithComponent);
    }
  }

}
