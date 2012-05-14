package eu.stratosphere.pact.programs.connected.tasks;

import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdate;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdateFlag;

public class UpdatedToUpdateFlag extends AbstractMinimalTask {

	@Override
	protected void initTask() {
	}

	@Override
	public void run() throws Exception {
		ComponentUpdate u = new ComponentUpdate();
		ComponentUpdateFlag uF = new ComponentUpdateFlag();
		
		while(inputs[0].next(u)) {
			uF.setCid(u.getCid());
			uF.setVid(u.getVid());
			uF.setUpdated(true);
			
			output.collect(uF);
		}
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

}
