package eu.stratosphere.pact.runtime.test.util;

import junit.framework.Assert;
import eu.stratosphere.nephele.template.AbstractTask;

public class TaskCancelThread extends Thread {

	final private int cancelTimeout;
	final private Thread interruptedThread;
	final private AbstractTask canceledTask;
	
	public TaskCancelThread(int cancelTimeout, Thread interruptedThread, AbstractTask canceledTask) {
		this.cancelTimeout = cancelTimeout;
		this.interruptedThread = interruptedThread;
		this.canceledTask = canceledTask;
	}
	
	@Override
	public void run() {
		try {
			Thread.sleep(this.cancelTimeout*1000);
		} catch (InterruptedException e) {
			Assert.fail("CancelThread interruped while waiting for cancel timeout");
		}
		
		try {
			this.canceledTask.cancel();
			this.interruptedThread.interrupt();
		} catch (Exception e) {
			Assert.fail("Canceling task failed");
		}
	}
	
}
