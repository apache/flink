package eu.stratosphere.pact.programs.connected.tasks;

import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdate;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdateAccessor;
import eu.stratosphere.pact.programs.connected.types.TransitiveClosureEntry;

public class InitialUpdates extends AbstractMinimalTask {

	@Override
	protected void initTask() {
		outputAccessors[0] = new ComponentUpdateAccessor();
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public void run() throws Exception {
		TransitiveClosureEntry tc = new TransitiveClosureEntry();
		ComponentUpdate update = new ComponentUpdate();
		
		long vid;
		long oldCid;
		
		while (inputs[0].next(tc))
		{	
			vid = tc.getVid();
			oldCid = tc.getCid();
			
			//Look if neighbour is smaller
			long cid = oldCid;
			int numNeighbours = tc.getNumNeighbors();
			long[] neighbourIds = tc.getNeighbors();
			for (int i = 0; i < numNeighbours; i++) {
				if(neighbourIds[i] < cid) {
					cid = neighbourIds[i];
				}
			}
			
			if(cid < oldCid) {
				update.setVid(vid);
				update.setCid(cid);
				output.collect(update);
			}
		}
	}

}
