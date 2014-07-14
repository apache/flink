package eu.stratosphere.streaming.api.invokable;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.types.Record;

public class DefaultTaskInvokable implements UserTaskInvokable {

  @Override
  public void invoke(Record record, RecordWriter<Record> output) throws Exception {}

}
