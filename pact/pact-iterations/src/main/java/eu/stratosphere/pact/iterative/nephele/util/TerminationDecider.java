package eu.stratosphere.pact.iterative.nephele.util;

import java.util.Iterator;

import eu.stratosphere.pact.common.type.PactRecord;

public abstract class TerminationDecider {
	
	public TerminationDecider() {
	}
	
	public abstract boolean decide(Iterator<PactRecord> values);
}
