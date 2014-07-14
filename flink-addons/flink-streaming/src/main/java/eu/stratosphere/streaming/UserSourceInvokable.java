package eu.stratosphere.streaming;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.io.RecordWriter;

public interface UserSourceInvokable {
  public void invoke(RecordWriter<IOReadableWritable> output) throws Exception ;
}
