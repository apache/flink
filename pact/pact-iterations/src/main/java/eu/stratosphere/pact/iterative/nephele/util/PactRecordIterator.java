package eu.stratosphere.pact.iterative.nephele.util;

import java.io.IOException;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.MutableObjectIterator;

public abstract class PactRecordIterator implements MutableObjectIterator<PactRecord> {

	private MutableObjectIterator<Value> iter;

	public PactRecordIterator(MutableObjectIterator<Value> iter) {
		this.iter = iter;
	}
	
	@Override
	public boolean next(PactRecord target) throws IOException {
		return nextPactRecord(iter, target);
	}

	public abstract boolean nextPactRecord(MutableObjectIterator<Value> iter, 
			PactRecord target) throws IOException;
}
