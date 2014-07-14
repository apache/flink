package eu.stratosphere.streaming.api;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.types.Record;

public class FaultTolerancyBuffer {

	private final static long TIMEOUT = 1000;
	private final static long TIMESTAMP_STORE_WINDOW = TIMEOUT / 10;
	
	private Long timeOfLastUpdate;
	private Map<String, StreamRecord> recordBuffer;
	private Map<String, Integer> ackCounter;
	private SortedMap<Long, Set<String>> recordsByTime;
	private Map<String, Long> recordTimestamps;
	
	private List<RecordWriter<Record>> outputs;
	private String channelID;

	private int numberOfOutputs;

	public FaultTolerancyBuffer(List<RecordWriter<Record>> outputs,
			String channelID) {
		this.timeOfLastUpdate = System.currentTimeMillis();
		this.outputs = outputs;
		this.recordBuffer = new HashMap<String, StreamRecord>();
		this.ackCounter = new HashMap<String, Integer>();
		this.numberOfOutputs = outputs.size();
		this.channelID = channelID;
		this.recordsByTime = new TreeMap<Long, Set<String>>();
		this.recordTimestamps=new HashMap<String, Long>();
	}

	public void addRecord(StreamRecord streamRecord) {

		recordBuffer.put(streamRecord.getId(), streamRecord);
		ackCounter.put(streamRecord.getId(), numberOfOutputs);
		addTimestamp(streamRecord.getId());
	}
	
	private void timeoutRecords() {
		Long currentTime = System.currentTimeMillis();
		
		if (timeOfLastUpdate + TIMEOUT < currentTime) {
			
			List<String> timedOutRecords = new LinkedList<String>();
			Map<Long, Set<String>> timedOut = recordsByTime.subMap(0L, currentTime - TIMEOUT);
			for (Set<String> recordSet : timedOut.values()) {
				if (!recordSet.isEmpty()) {
					for (String recordID : recordSet) {
						timedOutRecords.add(recordID);
					}
				}
			}
			
			recordsByTime.keySet().removeAll(timedOut.keySet());
			for (String recordID: timedOutRecords) {
				failRecord(recordID);
			}
		}
	}
	
	public void addTimestamp(String recordID) {
		Long currentTime = System.currentTimeMillis();
		recordTimestamps.put(recordID,currentTime);
		
		if (recordsByTime.containsKey(currentTime)) {
			recordsByTime.get(currentTime).add(recordID);
		} else {
			Set<String> recordSet = new HashSet<String>();
			recordSet.add(recordID);
			recordsByTime.put(currentTime,recordSet);
		}
		System.out.println(currentTime.toString()+" : "+recordsByTime.get(currentTime).toString());
	}

	public Record popRecord(String recordID) {
		System.out.println("Pop ID: "+recordID);
		Record record = recordBuffer.get(recordID)
				.getRecord();
		removeRecord(recordID);
		return record;
	}

	private void removeRecord(String recordID) {
		recordBuffer.remove(recordID);
		ackCounter.remove(recordID);
		recordsByTime.get(recordTimestamps.remove(recordID)).remove(recordID);
	}
	
	public void ackRecord(String recordID) {

		if (ackCounter.containsKey(recordID)) {
			int ackCount = ackCounter.get(recordID) - 1;

			if (ackCount == 0) {
				removeRecord(recordID);
			} else {
				ackCounter.put(recordID, ackCount);
			}
		}

	}

	public void failRecord(String recordID) {
		// Create new id to avoid double counting acks
		System.out.println("Fail ID: "+recordID);
		StreamRecord newRecord = new StreamRecord(popRecord(recordID), channelID)
				.addId();
		addRecord(newRecord);
		reEmit(newRecord.getRecordWithId());

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
