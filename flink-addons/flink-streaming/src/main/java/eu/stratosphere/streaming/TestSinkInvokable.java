package eu.stratosphere.streaming;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

public class TestSinkInvokable implements UserSinkInvokable {

	@Override
	public void invoke(Record record, RecordWriter<Record> output)
			throws Exception {
		
	    StringValue value = new StringValue("");
	    record.getFieldInto(0, value);
	    System.out.println(value.getValue());
	}

}
