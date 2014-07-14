package eu.stratosphere.streaming.test.wordcount;

import eu.stratosphere.streaming.api.StreamRecord;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

public class WordCountDummySource extends UserSourceInvokable {

	private String line = new String();
	private StringValue lineValue = new StringValue();
	private Value[] values = new Value[1];

	public WordCountDummySource() {

	}

	@Override
	public void invoke() throws Exception {
		line = "first one";
		lineValue.setValue(line);
		values[0] = lineValue;
		StreamRecord record = new StreamRecord(lineValue);

		emit(record);

		line = "second two";
		lineValue.setValue(line);
		values[0] = lineValue;
		record.setRecord(0, values);

		emit(record);
	}
}
