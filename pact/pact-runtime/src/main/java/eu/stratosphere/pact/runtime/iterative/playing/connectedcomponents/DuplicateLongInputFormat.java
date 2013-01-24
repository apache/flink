package eu.stratosphere.pact.runtime.iterative.playing.connectedcomponents;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

import java.util.regex.Pattern;

public class DuplicateLongInputFormat extends TextInputFormat {

  private static final Pattern SEPARATOR = Pattern.compile("[,\t ]");

  @Override
  public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
    String str = new String(bytes, offset, numBytes);
    long value = Long.parseLong(str);

    target.clear();
    target.addField(new PactLong(value));
    target.addField(new PactLong(value));

    return true;
  }
}