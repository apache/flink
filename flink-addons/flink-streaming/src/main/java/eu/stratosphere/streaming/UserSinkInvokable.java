package eu.stratosphere.streaming;

import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.types.Record;

public interface UserSinkInvokable {

  public void invoke(Record record,
      RecordReader<Record> input) throws Exception;
}