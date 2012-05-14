package eu.stratosphere.pact.programs.connected.tasks;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.iterative.nephele.util.TerminationDecider;

public class EmptyTerminationDecider extends TerminationDecider {
	protected static final Log LOG = LogFactory.getLog(EmptyTerminationDecider.class);
	
	PactLong count = new PactLong();
	
	@Override
	public boolean decide(Iterator<PactRecord> values) {
		long sum = 0;
		while(values.hasNext()) {
			PactRecord preSum = values.next();
			sum += preSum.getField(0, count).getValue();
		}
		
		if(sum == 0) {
			LOG.info("No updates -- terminate!");
			return true;
		} else {
			LOG.info(sum + " updates::::::");
			return false;
		}
	}

}
