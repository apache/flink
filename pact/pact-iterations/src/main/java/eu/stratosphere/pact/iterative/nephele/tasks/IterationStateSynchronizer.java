package eu.stratosphere.pact.iterative.nephele.tasks;

import static eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask.initStateTracking;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateEvent.ChannelState;
import eu.stratosphere.pact.iterative.nephele.util.ChannelStateTracker;
import eu.stratosphere.pact.iterative.nephele.util.StateChangeException;

public class IterationStateSynchronizer extends AbstractMinimalTask {
	
	private ChannelStateTracker[] stateListeners;
	protected static final Log LOG = LogFactory.getLog(IterationStateSynchronizer.class);
	private int count = 0;
	private long start = 0;
	
	
	@SuppressWarnings("unchecked")
	@Override
	protected void initTask() {
		int numInputs = getNumberOfInputs();
		stateListeners = new ChannelStateTracker[numInputs];
		
		for (int i = 0; i < numInputs; i++)
		{
			stateListeners[i] = 
					initStateTracking((InputGate<PactRecord>) getEnvironment().getInputGate(i));
		}
	}

	@Override
	public void run() throws Exception {
		MutableObjectIterator<Value> input = inputs[0];
		ChannelStateTracker stateListener = stateListeners[0];
		
		PactRecord rec = new PactRecord();
		PactString key = new PactString();
		PactLong value = new PactLong();
		
		while(true) {
			try {
				start = System.nanoTime();
				boolean success = input.next(rec);				
				if(success) {
					throw new RuntimeException("Received record");
				} else {
					//If it returned, but there is no state change the iterator is exhausted 
					// => Finishing
					break;
				}
			} catch (StateChangeException ex) {
				if(stateListener.isChanged() && stateListener.getState() == ChannelState.CLOSED) {
					LOG.info("Finnished Step: " + count + " in " + (System.nanoTime()-start)/1000000 + "");
					
					count++;
					getEnvironment().getInputGate(1).publishEvent(new ChannelStateEvent(ChannelState.CLOSED));
				} 
			}
		}
		
		//Satisfy nephele
		inputs[1].next(rec);
		
		output.close();
	}	

	@Override
	public int getNumberOfInputs() {
		return 2;
	}

}
