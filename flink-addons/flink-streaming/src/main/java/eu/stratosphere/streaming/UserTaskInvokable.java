package eu.stratosphere.streaming;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.types.Record;

public interface UserTaskInvokable {

  public void invoke(Record record,
      RecordWriter<Record> output) throws Exception;
}
