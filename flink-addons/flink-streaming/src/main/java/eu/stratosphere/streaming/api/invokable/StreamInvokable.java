package eu.stratosphere.streaming.api.invokable;

import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.streaming.api.StreamRecord;
import eu.stratosphere.types.Record;

public abstract class StreamInvokable {

	private List<RecordWriter<Record>> outputs;

	protected String channelID;
	private Map<String, StreamRecord> emittedRecords;

	public final void declareOutputs(List<RecordWriter<Record>> outputs,
			String channelID, Map<String, StreamRecord> emittedRecords) {
		this.outputs = outputs;
		this.channelID = channelID;
		this.emittedRecords = emittedRecords;
	}

	public final void emit(Record record) {		
		StreamRecord streamRecord = new StreamRecord(record, channelID).setId();
		for (RecordWriter<Record> output : outputs) {
			try {
				output.emit(streamRecord.getRecord());
				emittedRecords.put(streamRecord.getId(), streamRecord);
				
				System.out.println(this.getClass().getName());
				System.out.println("Emitted " + streamRecord.getId() + "-"
						+ streamRecord.toString());
				System.out.println("---------------------");
				
			} catch (Exception e) {
				System.out.println("Emit error");
			}
		}
	}
}