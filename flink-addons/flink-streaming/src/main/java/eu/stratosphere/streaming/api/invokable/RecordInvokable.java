package eu.stratosphere.streaming.api.invokable;

import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public interface RecordInvokable {
	public void invoke(StreamRecord record) throws Exception;
}
