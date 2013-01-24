package eu.stratosphere.pact.runtime.iterative.playing.pagerank;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;

public class DotProductMatch2 extends MatchStub {

  private PactRecord record;

  @Override
  public void open(Configuration parameters) throws Exception {
    record = new PactRecord();
  }

  @Override
  public void match(PactRecord transitionMatrixEntry, PactRecord pageWithRank, Collector<PactRecord> collector)
      throws Exception {

//    System.out.println("fields ###### " + pageWithRank.getNumFields() + " " + transitionMatrixEntry.getNumFields());
//    System.out.println("field0 ###### " + pageWithRank.getField(0, PactLong.class).getValue() + " " + transitionMatrixEntry.getField(0, PactLong.class).getValue());
//    System.out.println("field1 ###### " + pageWithRank.getField(1, PactDouble.class).getValue() + " " + transitionMatrixEntry.getField(1, PactDouble.class).getValue());

//    long source = transitionMatrixEntry.getField(0, PactLong.class).getValue();
//    long target = transitionMatrixEntry.getField(1, PactLong.class).getValue();
//    long vertexID = pageWithRank.getField(0, PactLong.class).getValue();

    double rank = pageWithRank.getField(1, PactDouble.class).getValue();
    double transitionProbability = transitionMatrixEntry.getField(2, PactDouble.class).getValue();

    record.setField(0, transitionMatrixEntry.getField(1, PactLong.class));
    record.setField(1, new PactDouble(rank * transitionProbability));

//    System.out.println("Joining (" + vertexID + "," + rank + ") with (" + source + "," + target + "," + transitionProbability + ")");
//    System.out.println(">>>>>>>>>>>> Emitting: " + target + "," + (rank * transitionProbability));


    collector.collect(record);
  }

  @Override
  public void close() throws Exception {
    record = null;
  }
}