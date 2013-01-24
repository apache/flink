package eu.stratosphere.pact.runtime.iterative.playing.connectedcomponents.rowpartitioned;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

public class NeighborsComponentIDToWorksetMatch extends MatchStub {

  private PactRecord result;
  private PactLong neighbor;

  @Override
  public void open(Configuration parameters) throws Exception {
    result = new PactRecord();
    neighbor = new PactLong();
  }

  @Override
  public void match(PactRecord vertexWithComponent, PactRecord adjacencyList, Collector<PactRecord> out)
      throws Exception {

    result.setField(1, vertexWithComponent.getField(1, PactLong.class));

    long[] neighborIDs = adjacencyList.getField(1, AdjacencyList.class).neighborIDs();

    for (long neighborID : neighborIDs) {
      neighbor.setValue(neighborID);
      result.setField(0, neighbor);
      out.collect(result);
    }
  }
}
