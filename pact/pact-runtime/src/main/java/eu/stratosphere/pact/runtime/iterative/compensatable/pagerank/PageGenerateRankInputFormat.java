package eu.stratosphere.pact.runtime.iterative.compensatable.pagerank;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.runtime.iterative.compensatable.ConfigUtils;

import java.util.regex.Pattern;

public class PageGenerateRankInputFormat extends TextInputFormat {

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
    long vertexID = Long.parseLong(SEPARATOR.split(str)[0]);

    target.clear();
    target.addField(new PactLong(vertexID));
    target.addField(initialRank);

    return true;
  }
}
