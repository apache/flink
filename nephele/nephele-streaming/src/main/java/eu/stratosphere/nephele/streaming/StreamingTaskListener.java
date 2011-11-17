package eu.stratosphere.nephele.streaming;

import eu.stratosphere.nephele.io.InputGateListener;
import eu.stratosphere.nephele.io.OutputGateListener;
import eu.stratosphere.nephele.types.Record;

public class StreamingTaskListener implements InputGateListener, OutputGateListener {

	@Override
	public void channelCapacityExhausted(int channelIndex) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void recordEmitted(Record record) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void waitingForAnyChannel() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void recordReceived(Record record) {
		// TODO Auto-generated method stub
		
	}

}
