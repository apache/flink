/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.iterative.concurrent;

/**
 * Latch used to wait for the previous superstep to complete.
 */
public class SuperstepKickoffLatch {

	private final Object monitor = new Object();

	private int superstepNumber = 1;

	private boolean terminated;

	public void triggerNextSuperstep() {
		synchronized (monitor) {
			if (terminated) {
				throw new IllegalStateException("Already terminated.");
			}
			superstepNumber++;
			monitor.notifyAll();
		}
	}

	public void signalTermination() {
		synchronized (monitor) {
			terminated = true;
			monitor.notifyAll();
		}
	}

	public boolean awaitStartOfSuperstepOrTermination(int superstep) throws InterruptedException {
		while (true) {
			synchronized (monitor) {
				if (terminated) {
					return true;
				}
				else if (superstepNumber == superstep) {
					// reached the superstep. all good!
					return false;
				}
				else if (superstepNumber == superstep - 1) {
					monitor.wait(2000);
				}
				else {
					throw new IllegalStateException("Error while waiting for start of next superstep. current= " + superstepNumber + " waitingFor=" + superstep);
				}
			}
		}
	}
}
