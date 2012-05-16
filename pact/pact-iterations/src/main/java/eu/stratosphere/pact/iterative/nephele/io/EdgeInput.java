package eu.stratosphere.pact.iterative.nephele.io;

import com.google.common.base.Charsets;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

import java.util.regex.Pattern;

public class EdgeInput extends DelimitedInputFormat {

  private static final Pattern SEPARATOR = Pattern.compile(",");

  @Override
  public boolean readRecord(PactRecord target, byte[] bytes, int numBytes) {
    String[] ids = SEPARATOR.split(new String(bytes, Charsets.UTF_8));
    target.setField(0, new PactInteger(Integer.parseInt(ids[0])));
    target.setField(1, new PactInteger(Integer.parseInt(ids[1])));
    return true;
  }

}
