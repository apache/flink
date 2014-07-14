package eu.stratosphere.streaming;


import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

public class TestSourceInvokable implements UserSourceInvokable {

  @Override
  public void invoke(RecordWriter<Record> output) throws Exception {
    for (int i = 0; i < 10; i++) {
      // output.emit(new StringRecord(rnd.nextInt(10)+" "+rnd.nextInt(1000)));
    	output.emit(new Record(new StringValue("5 500")));//new StringRecord("5 500"));
    	output.emit(new Record(new StringValue("4 500")));
    }
  }

}
