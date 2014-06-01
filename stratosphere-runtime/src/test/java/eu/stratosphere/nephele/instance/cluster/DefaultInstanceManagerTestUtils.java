/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.instance.cluster;

import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This class contains utility methods used during the tests of the {@link eu.stratosphere.nephele.instance.DefaultInstanceManager} implementation.
 * 
 */
public class DefaultInstanceManagerTestUtils {

	/**
	 * Granularity of the sleep time.
	 */
	private static final long SLEEP_TIME = 10; // 10 milliseconds

	/**
	 * Private constructor so the class cannot be instantiated.
	 */
	private DefaultInstanceManagerTestUtils() {
	}

	/**
	 * Waits until a specific number of instances have registered or deregistrations with the given
	 * {@link InstanceListener} object for a given job or the maximum wait time has elapsed.
	 * 
	 * @param jobID
	 *        the ID of the job to check the instance registration for
	 * @param instanceListener
	 *        the listener which shall be notified when a requested instance is available for the job
	 * @param numberOfInstances
	 *        the number of registered instances to wait for
	 * @param maxWaitTime
	 *        the maximum wait time before this method returns
	 */
	public static void waitForInstances(JobID jobID, TestInstanceListener instanceListener,
			int numberOfInstances, long maxWaitTime) {

		final long startTime = System.currentTimeMillis();

		while (instanceListener.getNumberOfAllocatedResourcesForJob(jobID) != numberOfInstances) {
			try {
				Thread.sleep(SLEEP_TIME);
			} catch (InterruptedException e) {
				break;
			}

			if ((System.currentTimeMillis() - startTime) >= maxWaitTime) {
				break;
			}
		}
	}
}
