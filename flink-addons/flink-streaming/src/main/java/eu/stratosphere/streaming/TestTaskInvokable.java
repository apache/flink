package eu.stratosphere.streaming;

import java.util.List;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.streaming.cellinfo.WorkerEngineExact;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

public class TestTaskInvokable implements UserTaskInvokable {

  private WorkerEngineExact engine = new WorkerEngineExact(10, 1000, 0);

  @Override
  public void invoke(Record record,
      RecordWriter<Record> output) throws Exception {
  		StringValue value = new StringValue();
  		record.getFieldInto(0, value);
  		String[] values = value.getValue().split(" ");
  		
  		//INFO
  		if (values.length == 2)
  		{
  			engine.put(Integer.parseInt(values[0]), Long.parseLong(values[1]));
  			output.emit(new Record(new StringValue(values[0] + " " + values[1])));
  		}
  		//QUERY
  		else if (values.length == 3)
  		{
  			output.emit(new Record(new StringValue(String.valueOf(engine.get(
          Long.parseLong(values[1]), Long.parseLong(values[2]),
          Integer.parseInt(values[0]))))));
  		}
  		
//  	RecordReader<IOReadableWritable> input1= inputs.get(0);
//    RecordReader<IOReadableWritable> input2= inputs.get(0);
//
//    
//    while (input1.hasNext() && input2.hasNext()) {
//      String[] info = input1.next().toString().split(" ");
//      String[] query = input2.next().toString().split(" ");
//
//      engine.put(Integer.parseInt(info[0]), Long.parseLong(info[1]));
//
//      output.emit(new StringRecord(info[0] + " " + info[1]));
//      output.emit(new StringRecord(String.valueOf(engine.get(
//          Long.parseLong(query[1]), Long.parseLong(query[2]),
//          Integer.parseInt(query[0])))));
//    }
//    while (inputs.get(0).hasNext()) {
//
//      IOReadableWritable info = inputs.get(0).next();
//
//      output.emit(info);
//    }
//    while (inputs.get(1).hasNext()) {
//
//      IOReadableWritable query = inputs.get(1).next();
//
//      output.emit(query);
//    }
    
  }


}
