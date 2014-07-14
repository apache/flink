package eu.stratosphere.streaming;

import java.util.List;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;

public interface UserTaskInvokable {

  public void invoke(List<RecordReader<IOReadableWritable>> inputs,
      RecordWriter<IOReadableWritable> output) throws Exception;

}
