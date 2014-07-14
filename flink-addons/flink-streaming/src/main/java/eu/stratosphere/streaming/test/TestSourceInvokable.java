package eu.stratosphere.streaming.test;


import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;

public class TestSourceInvokable implements UserSourceInvokable {

  @Override
  public void invoke(RecordWriter<Record> output) throws Exception {
    for (int i = 0; i < 10; i++) {
    	Record record1 = new Record(2);
    	record1.setField(0, new IntValue(5));
    	record1.setField(1, new LongValue(500));
    	Record record2 = new Record(2);
    	record2.setField(0, new IntValue(4));
    	record2.setField(1, new LongValue(500));
    	output.emit(record1);
    	output.emit(record2);
    }
  }

}
