package eu.stratosphere.streaming.test.wordcount;

import eu.stratosphere.streaming.api.StreamRecord;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

public class WordCountDummySource extends UserSourceInvokable {

	private String line = new String();
	private StringValue lineValue = new StringValue();
	private Value[] values = new StringValue[1];

	public WordCountDummySource() {
		line = "first second";
		lineValue.setValue(line);
		values[0] = lineValue;
	}

	@Override
	public void invoke() throws Exception {
		for (int i = 0; i < 1; i++) {
			emit(new StreamRecord(values));
		System.out.println("xxxxxxxxx");
		}
	}
}
