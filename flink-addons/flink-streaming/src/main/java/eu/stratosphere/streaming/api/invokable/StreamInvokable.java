package eu.stratosphere.streaming.api.invokable;

import java.util.List;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.streaming.api.FaultToleranceBuffer;
import eu.stratosphere.streaming.api.StreamRecord;

public abstract class StreamInvokable {

	private List<RecordWriter<StreamRecord>> outputs;

	protected String channelID;
	private FaultToleranceBuffer emittedRecords;

	public final void declareOutputs(List<RecordWriter<StreamRecord>> outputs,
			String channelID, FaultToleranceBuffer emittedRecords) {
		this.outputs = outputs;
		this.channelID = channelID;
		this.emittedRecords = emittedRecords;
	}

	public final void emit(StreamRecord record) {

		record.setId(channelID);
		emittedRecords.addRecord(record);

		for (RecordWriter<StreamRecord> output : outputs) {
			try {

				output.emit(record);

				System.out.println(this.getClass().getName());
				System.out.println("Emitted " + record.getId() + "-"
						+ record.toString());
				System.out.println("---------------------");

			} catch (Exception e) {
				System.out.println("Emit error: " + e.getMessage());
				emittedRecords.failRecord(record.getId());
			}
		}
	}
}