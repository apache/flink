package eu.stratosphere.pact.iterative.nephele.io;

import java.io.IOException;

import com.google.common.base.Charsets;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class EdgeOutput extends FileOutputFormat {
  @Override
  public void writeRecord(PactRecord record) throws IOException {
    PactInteger a = record.getField(0, PactInteger.class);
    PactInteger b = record.getField(1, PactInteger.class);
    stream.write((a.getValue() + "," + b.getValue() + "\n").getBytes(Charsets.UTF_8));
  }
}
