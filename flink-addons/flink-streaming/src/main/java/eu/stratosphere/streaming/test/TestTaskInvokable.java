package eu.stratosphere.streaming.test;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.test.cellinfo.WorkerEngineExact;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

public class TestTaskInvokable implements UserTaskInvokable {

	private WorkerEngineExact engine = new WorkerEngineExact(10, 1000, 0);

	@Override
	public void invoke(Record record, RecordWriter<Record> output)
			throws Exception {
		IntValue value1 = new IntValue(0);
		record.getFieldInto(0, value1);
		LongValue value2 = new LongValue(0);
		record.getFieldInto(1, value2);

		// INFO
		if (record.getNumFields() == 2) {
			engine.put(value1.getValue(), value2.getValue());
			output.emit(new Record(new StringValue(value1 + " " + value2)));
		}
		// QUERY
		else if (record.getNumFields() == 3) {
			LongValue value3 = new LongValue(0);
			record.getFieldInto(2, value3);

			output.emit(new Record(new StringValue(String.valueOf(engine.get(
					value2.getValue(), value3.getValue(), value1.getValue())))));
		}
	}
}
