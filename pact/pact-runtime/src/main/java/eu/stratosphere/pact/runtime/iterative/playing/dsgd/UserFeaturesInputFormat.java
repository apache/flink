package eu.stratosphere.pact.runtime.iterative.playing.dsgd;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

import java.util.regex.Pattern;

public class UserFeaturesInputFormat extends TextInputFormat {

  private static final Pattern SEPARATOR = Pattern.compile("[, \t]");

  private static final BiasedMatrixFactorization factorizer = new BiasedMatrixFactorization();

  @Override
  public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
    String str = new String(bytes, offset, numBytes);
    String[] parts = SEPARATOR.split(str);

    target.clear();
    target.addField(new PactInteger(Integer.parseInt(parts[0])));
    target.addField(new DenseVector(factorizer.initializeUserFeatures(3.1)));

    return true;
  }
}
