package eu.stratosphere.pact.runtime.iterative.compensatable.connectedcomponents;

import com.google.common.base.Charsets;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

import java.io.IOException;

public class ConnectedComponentsOutFormat extends FileOutputFormat {

  private final StringBuilder buffer = new StringBuilder();

  @Override
  public void writeRecord(PactRecord record) throws IOException {
    buffer.setLength(0);
    buffer.append(record.getField(0, PactLong.class).toString());
    buffer.append('\t');
    buffer.append(record.getField(1, PactLong.class).toString());
    buffer.append('\n');

    byte[] bytes = buffer.toString().getBytes(Charsets.UTF_8);
    stream.write(bytes);
  }
}