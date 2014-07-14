package eu.stratosphere.streaming;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.types.Record;

public class DefaultSourceInvokable implements UserSourceInvokable {

  @Override
  public void invoke(RecordWriter<Record> output) throws Exception {}

}
