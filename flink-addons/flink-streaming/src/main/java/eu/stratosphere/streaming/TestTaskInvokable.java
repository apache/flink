package eu.stratosphere.streaming;

import java.util.List;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.streaming.cellinfo.WorkerEngineExact;

public class TestTaskInvokable implements UserTaskInvokable {

  private WorkerEngineExact engine = new WorkerEngineExact(10, 1000, 0);

  @Override
  public void invoke(List<RecordReader<IOReadableWritable>> inputs,
      RecordWriter<IOReadableWritable> output) throws Exception {
    RecordReader<IOReadableWritable> input1= inputs.get(0);
    RecordReader<IOReadableWritable> input2= inputs.get(0);

    while (input1.hasNext() && input2.hasNext()) {
      String[] info = input1.next().toString().split(" ");
      String[] query = input2.next().toString().split(" ");

      engine.put(Integer.parseInt(info[0]), Long.parseLong(info[1]));

      output.emit(new StringRecord(info[0] + " " + info[1]));
      output.emit(new StringRecord(String.valueOf(engine.get(
          Long.parseLong(query[1]), Long.parseLong(query[2]),
          Integer.parseInt(query[0])))));
    }
    while (inputs.get(0).hasNext()) {

      IOReadableWritable info = inputs.get(0).next();

      output.emit(info);
    }
    while (inputs.get(1).hasNext()) {

      IOReadableWritable query = inputs.get(1).next();

      output.emit(query);
    }
    
  }


}
