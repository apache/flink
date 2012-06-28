package eu.stratosphere.pact.runtime.iterative.playing;

import com.google.common.base.Charsets;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

import java.io.IOException;

@Deprecated
public class PlayOutFormat extends FileOutputFormat {

  private final StringBuilder buffer = new StringBuilder();

  @Override
  public void writeRecord(PactRecord record) throws IOException {
    System.out.println("GOT record");
    buffer.setLength(0);
    buffer.append(record.getField(0, PactString.class).toString());
    buffer.append('\n');

    byte[] bytes = buffer.toString().getBytes(Charsets.UTF_8);
    stream.write(bytes);
  }
}
