package eu.stratosphere.streaming.api;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;

public class AckEvent extends AbstractTaskEvent {
	private Long recordId;
	
	public AckEvent(Long recordId) {
		setRecordId(recordId);
		System.out.println("created " + recordId);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {}

	@Override
	public void read(DataInput in) throws IOException {}
	
	public void setRecordId(Long recordId) {
		this.recordId = recordId;
	}
	public Long getRecordId() {
		return this.recordId;
	}
}