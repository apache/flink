package eu.stratosphere.pact.programs.inputs;

import com.google.common.base.Charsets;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

import java.util.regex.Pattern;

public class TwitterLinkInput extends DelimitedInputFormat {

  static final Pattern SEPARATOR = Pattern.compile("\t");

  @Override
  public boolean readRecord(PactRecord target, byte[] bytes, int numBytes) {
    String[] triple = SEPARATOR.split(new String(bytes, Charsets.UTF_8));
    target.setField(1, new PactLong(Long.parseLong(triple[0])));
    target.setField(0, new PactLong(Long.parseLong(triple[1])));
    return true;
  }

}
