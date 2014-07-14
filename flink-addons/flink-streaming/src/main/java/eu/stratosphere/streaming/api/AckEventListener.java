package eu.stratosphere.streaming.api;

import java.util.Map;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;

public class AckEventListener implements EventListener {

	private String taskInstanceID;
	private Map<String, StreamRecord> recordBuffer;

	public AckEventListener(String taskInstanceID,Map<String, StreamRecord> recordBuffer) {
		this.taskInstanceID=taskInstanceID;
		this.recordBuffer=recordBuffer;
	}

	public void eventOccurred(AbstractTaskEvent event) {
		AckEvent ackEvent = (AckEvent) event;
		String recordId = ackEvent.getRecordId();
		String ackCID = recordId.split("-", 2)[0];
		if (ackCID.equals(taskInstanceID)) {
			System.out.println("Ack recieved " + ackEvent.getRecordId());
			recordBuffer.remove(ackEvent.getRecordId());
			System.out.println(recordBuffer);
			System.out.println("---------------------");

		}

	}
}
