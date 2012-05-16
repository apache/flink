package eu.stratosphere.pact.programs.inputs;

import com.google.common.base.Charsets;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

import java.util.regex.Pattern;

public class DBPediaPageLinkInput extends DelimitedInputFormat {

  static final Pattern SEPARATOR = Pattern.compile(" ");

  @Override
  public boolean readRecord(PactRecord target, byte[] bytes, int numBytes) {
    String[] triple = SEPARATOR.split(new String(bytes, Charsets.UTF_8));
    target.setField(0, new PactString(triple[0].substring(triple[0].lastIndexOf('/') + 1, triple[0].length() - 1)));
    target.setField(1, new PactString(triple[2].substring(triple[2].lastIndexOf('/') + 1, triple[2].length() - 1)));
    return true;
  }

}
