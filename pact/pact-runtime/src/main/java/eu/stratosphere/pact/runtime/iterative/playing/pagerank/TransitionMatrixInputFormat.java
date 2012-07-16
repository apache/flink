package eu.stratosphere.pact.runtime.iterative.playing.pagerank;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;

import java.util.regex.Pattern;

public class TransitionMatrixInputFormat extends TextInputFormat {

  private static final Pattern SEPARATOR = Pattern.compile(",");

  @Override
  public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
    String str = new String(bytes, offset, numBytes);
    String[] parts = SEPARATOR.split(str);

    target.clear();
    target.addField(new PactLong(Long.parseLong(parts[0])));
    target.addField(new PactLong(Long.parseLong(parts[1])));
    target.addField(new PactDouble(Double.parseDouble(parts[2])));

    return true;
  }
}
