package eu.stratosphere.pact.runtime.iterative.playing.dsgd;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;

public class CollectInteractionMatch extends MatchStub {

  private PactRecord result = new PactRecord();

  @Override
  public void match(PactRecord itemFeatures, PactRecord interaction, Collector<PactRecord> out) throws Exception {

    interaction.copyTo(result);
    result.addField(itemFeatures.getField(1, DenseVector.class));

    out.collect(result);
  }
}
