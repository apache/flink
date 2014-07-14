package eu.stratosphere.streaming;

import java.util.List;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.types.Record;

public interface UserTaskInvokable {

  public void invoke(Record record,
      RecordWriter<Record> output) throws Exception;

}
