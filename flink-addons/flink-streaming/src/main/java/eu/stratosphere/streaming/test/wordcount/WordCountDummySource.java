package eu.stratosphere.streaming.test.wordcount;

import eu.stratosphere.streaming.api.StreamRecord;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.types.StringValue;

public class WordCountDummySource extends UserSourceInvokable {

	private String line = new String();
	private StringValue lineValue = new StringValue();

	public WordCountDummySource() {
		line = "first second";
		lineValue.setValue(line);
	}

	@Override
	public void invoke() throws Exception {
		for (int i = 0; i < 1; i++) {
			emit(new StreamRecord(lineValue));
		}
	}
}
