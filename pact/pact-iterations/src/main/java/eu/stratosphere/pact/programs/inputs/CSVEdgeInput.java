package eu.stratosphere.pact.programs.inputs;

import com.google.common.base.Charsets;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

import java.util.regex.Pattern;

public class CSVEdgeInput extends DelimitedInputFormat {

  PactLong node = new PactLong();
  PactLong neighbour = new PactLong();

  static final Pattern SEPARATOR = Pattern.compile("\t");

  @Override
  public boolean readRecord(PactRecord target, byte[] bytes, int numBytes) {
    String[] pair = SEPARATOR.split(new String(bytes, Charsets.UTF_8));
    node.setValue(Long.valueOf(pair[1]));
    neighbour.setValue(Long.valueOf(pair[0]));

    target.setField(0, node);
    target.setField(1, neighbour);
    return true;
  }

}
