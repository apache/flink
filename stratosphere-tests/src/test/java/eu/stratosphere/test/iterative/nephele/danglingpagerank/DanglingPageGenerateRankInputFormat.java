package eu.stratosphere.test.iterative.nephele.danglingpagerank;

import eu.stratosphere.api.record.io.TextInputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.iterative.nephele.ConfigUtils;
import eu.stratosphere.types.PactDouble;
import eu.stratosphere.types.PactLong;
import eu.stratosphere.types.PactRecord;

import java.util.regex.Pattern;

public class DanglingPageGenerateRankInputFormat extends TextInputFormat {
  private static final long serialVersionUID = 1L;

  private PactDouble initialRank;

  private static final Pattern SEPARATOR = Pattern.compile("[, \t]");

  @Override
  public void configure(Configuration parameters) {
    long numVertices = ConfigUtils.asLong("pageRank.numVertices", parameters);
    initialRank = new PactDouble(1 / (double) numVertices);
    super.configure(parameters);
  }

  @Override
  public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
    String str = new String(bytes, offset, numBytes);

    String[] tokens = SEPARATOR.split(str);

    long vertexID = Long.parseLong(tokens[0]);
    boolean isDangling = tokens.length > 1 && Integer.parseInt(tokens[1]) == 1;

    target.clear();
    target.addField(new PactLong(vertexID));
    target.addField(initialRank);
    target.addField(new BooleanValue(isDangling));

    return true;
  }
}
