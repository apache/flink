package eu.stratosphere.pact.runtime.iterative.playing.dsgd;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class UpdateParametersMatch extends MatchStub {

  private BiasedMatrixFactorization factorizer = new BiasedMatrixFactorization();

  private PactRecord result = new PactRecord();

  @Override
  public void match(PactRecord interactionWithItemFeatures, PactRecord userFeatures, Collector<PactRecord> out)
      throws Exception {

    double[] userFeatureVector = userFeatures.getField(1, DenseVector.class).values();
    DenseVector itemFeatureVector = interactionWithItemFeatures.getField(3, DenseVector.class);
    float rating = (float) interactionWithItemFeatures.getField(2, PactDouble.class).getValue();

    factorizer.updateParameters(userFeatureVector, itemFeatureVector.values(), rating, 0.05);

    result.setField(0, interactionWithItemFeatures.getField(1, PactInteger.class));
    result.setField(1, itemFeatureVector);

    out.collect(result);
  }
}
