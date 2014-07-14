package eu.stratosphere.streaming.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.types.Record;

public class FaultTolerancyBuffer {

	private Map<String, StreamRecord> recordBuffer;
	private Map<String, Integer> ackCounter;
	private List<RecordWriter<Record>> outputs;

	private int numberOfOutputs;

	public FaultTolerancyBuffer(List<RecordWriter<Record>> outputs) {
		this.recordBuffer = new HashMap<String, StreamRecord>();
		this.ackCounter = new HashMap<String, Integer>();
		this.numberOfOutputs = outputs.size();

	}

	public void addRecord(StreamRecord streamRecord) {

		recordBuffer.put(streamRecord.getId(), streamRecord);
		ackCounter.put(streamRecord.getId(), numberOfOutputs);
	}

	public Record popRecord(String recordID) {
		Record record = recordBuffer.get(recordID).getRecord();
		recordBuffer.remove(recordID);
		ackCounter.remove(recordID);
		return record;
	}

	public void ackRecord(String recordID) {

		if (ackCounter.containsKey(recordID)) {
			int ackCount = ackCounter.get(recordID) - 1;
			ackCounter.put(recordID, ackCount);

			if (ackCount == 0) {
				recordBuffer.remove(recordID);
				ackCounter.remove(recordID);
			}
		}

	}

	public void failRecord(String recordID) {
		// Create new id to avoid double counting acks
		StreamRecord newRecord = new StreamRecord(popRecord(recordID)).addId();
		addRecord(newRecord);
		reEmit(newRecord.getRecord());

	}

	public void reEmit(Record record) {
		for (RecordWriter<Record> output : outputs) {
			try {
				output.emit(record);
			} catch (Exception e) {
				System.out.println("Re-emit failed");
			}
		}

	}

	public Map<String, StreamRecord> getRecordBuffer() {
		return this.recordBuffer;
	}

}
