package eu.stratosphere.pact.programs.connected.tasks;

import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;
import eu.stratosphere.pact.programs.connected.types.TransitiveClosureEntry;

public class InitialStateComponents extends AbstractMinimalTask {

	@Override
	protected void initTask() {
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void run() throws Exception {
		TransitiveClosureEntry tc = new TransitiveClosureEntry();
		
		while (inputs[0].next(tc))
		{	
			tc.setCid(tc.getVid());
			output.collect(tc);
		}
	}

}
