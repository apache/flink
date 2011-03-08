package eu.stratosphere.pact.runtime.test.util;

import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class DelayingInfinitiveInputIterator extends InfiniteInputIterator {

	private int delay;
	
	public DelayingInfinitiveInputIterator(int delay) {
		this.delay = delay;
	}
	
	@Override
	public KeyValuePair<PactInteger, PactInteger> next() {
		try {
			Thread.sleep(delay);
		} catch (InterruptedException e) { }
		return super.next();
	}
	
}
