package eu.stratosphere.pact.programs.preparation.tasks;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;

public class Undirect extends AbstractMinimalTask {
	PactRecord record = new PactRecord();
	PactRecord result = new PactRecord();
	
	
	@Override
	protected void initTask() {
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void run() throws Exception {
		MutableObjectIterator<Value> input = inputs[0];
		PactLong pid = new PactLong();
		PactLong nid = new PactLong();
		
		while(input.next(record)) {
			pid = record.getField(0, pid);
			nid = record.getField(1, nid);
			
			output.collect(record);
			result.setField(0, nid);
			result.setField(1, pid);
			output.collect(result);
		}
	}

}
