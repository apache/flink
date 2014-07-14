package eu.stratosphere.streaming.api;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;

public class FailEventListener implements EventListener {

	private String taskInstanceID;
	private FaultTolerancyBuffer recordBuffer;

	public FailEventListener(String taskInstanceID,
			FaultTolerancyBuffer recordBuffer) {
		this.taskInstanceID = taskInstanceID;
		this.recordBuffer = recordBuffer;
	}

	public void eventOccurred(AbstractTaskEvent event) {
		FailEvent failEvent = (FailEvent) event;
		String recordId = failEvent.getRecordId();
		String failCID = recordId.split("-", 2)[0];
		if (failCID.equals(taskInstanceID)) {
			System.out.println("Fail recieved " + recordId);
			recordBuffer.failRecord(recordId);
			System.out.println(recordBuffer.getRecordBuffer());
			System.out.println("---------------------");

		}

	}
}
