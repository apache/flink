package eu.stratosphere.streaming.api.invokable;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

public class DefaultSourceInvokable implements UserSourceInvokable {

	private String motto = "Stratosphere -- Big Data looks tiny from here";
	private String[] mottoArray = motto.split(" ");

	@Override
	public void invoke(RecordWriter<Record> output) throws Exception {
		for (CharSequence word : mottoArray) {
			output.emit(new Record(new StringValue(word)));
		}
	}

}
