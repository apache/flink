package eu.stratosphere.nephele.taskmanager.transferenvelope;

import java.util.Queue;

import org.junit.Test;

import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;

public class SpillingQueueTest {

	@Test
	public void testCapacity() {
		
		final JobID jobID = new JobID();
		final ChannelID channelID = new ChannelID();
		
		
		int count = 0;
		
		try {
		
			final Queue<TransferEnvelope> queue = new SpillingQueue(new AbstractID(){}, null);
			//final Queue<TransferEnvelope> queue = new ArrayDeque<TransferEnvelope>();
			//final Queue<TransferEnvelope> queue = new LinkedList<TransferEnvelope>();
			
			while(true) {
				
				final TransferEnvelope te = new TransferEnvelope(count++, jobID, channelID);
				queue.add(te);
			}
			
		} catch(Throwable t) {
			System.gc();
		}
		
		System.out.println("Inserted " + count + " elements");
	}
}
