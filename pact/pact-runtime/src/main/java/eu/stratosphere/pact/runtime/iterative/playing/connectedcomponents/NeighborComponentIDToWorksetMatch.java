package eu.stratosphere.pact.runtime.iterative.playing.connectedcomponents;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.runtime.iterative.concurrent.IterationContext;

public class NeighborComponentIDToWorksetMatch extends MatchStub {

  private PactRecord result;
  private int indexInSubtaskGroup;
  private long elementsInWorkset;

  @Override
  public void open(Configuration parameters) throws Exception {
    result = new PactRecord();
    elementsInWorkset = 0;
    indexInSubtaskGroup = parameters.getInteger("pact.parallel.task.id", -1);
    if (indexInSubtaskGroup == -1) {
      throw new IllegalStateException();
    }
  }

  @Override
  public void match(PactRecord vertexWithComponent, PactRecord edge, Collector<PactRecord> out)
      throws Exception {

    result.setField(0, edge.getField(1, PactLong.class));
    result.setField(1, vertexWithComponent.getField(1, PactLong.class));

    elementsInWorkset++;

//    long sourceVertexID = edge.getField(0, PactLong.class).getValue();
//    long targetVertexID = edge.getField(1, PactLong.class).getValue();
//    long candidateComponentID = vertexWithComponent.getField(1, PactLong.class).getValue();

//    System.out.println("-------------- Sending component " + candidateComponentID +" of vertex " + sourceVertexID +  " to " + targetVertexID);

    out.collect(result);
  }

  @Override
  public void close() throws Exception {
    IterationContext.instance().incrementCount(indexInSubtaskGroup, elementsInWorkset);
  }
}
