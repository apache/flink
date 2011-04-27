/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.test.util;

import junit.framework.Assert;
import eu.stratosphere.nephele.template.AbstractInvokable;

public class TaskCancelThread extends Thread {

	final private int cancelTimeout;
	final private Thread interruptedThread;
	final private AbstractInvokable canceledTask;
	
	public TaskCancelThread(int cancelTimeout, Thread interruptedThread, AbstractInvokable canceledTask) {
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
