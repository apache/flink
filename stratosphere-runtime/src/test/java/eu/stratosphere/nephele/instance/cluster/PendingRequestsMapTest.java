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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeFactory;

/**
 * This class checks the {@link PendingRequestsMap} data structure.
 * 
 * @author warneke
 */
public class PendingRequestsMapTest {

	/**
	 * The first instance type used in the tests.
	 */
	private static final InstanceType INSTANCE_TYPE1 = InstanceTypeFactory.construct("test1", 1, 1, 2, 2, 0);

	/**
	 * The second instance type used in the tests.
	 */
	private static final InstanceType INSTANCE_TYPE2 = InstanceTypeFactory.construct("test2", 2, 2, 4, 4, 0);

	/**
	 * Checks the correctness of the {@link PendingRequestsMap} data structure.
	 */
	@Test
	public void testPendingRequestsMap() {

		final PendingRequestsMap prm = new PendingRequestsMap();

		assertFalse(prm.hasPendingRequests());

		prm.addRequest(INSTANCE_TYPE1, 1);
		prm.addRequest(INSTANCE_TYPE2, 2);
		prm.addRequest(INSTANCE_TYPE2, 2);

		assertTrue(prm.hasPendingRequests());

		final Iterator<Map.Entry<InstanceType, Integer>> it = prm.iterator();
		int iterationCounter = 0;
		while (it.hasNext()) {

			final Map.Entry<InstanceType, Integer> entry = it.next();
			++iterationCounter;

			if (entry.getKey().equals(INSTANCE_TYPE1)) {
				assertEquals(1, entry.getValue().intValue());
			}

			if (entry.getKey().equals(INSTANCE_TYPE2)) {
				assertEquals(4, entry.getValue().intValue());
			}
		}

		assertEquals(2, iterationCounter);

		prm.decreaseNumberOfPendingInstances(INSTANCE_TYPE1);
		prm.decreaseNumberOfPendingInstances(INSTANCE_TYPE1);
		prm.decreaseNumberOfPendingInstances(INSTANCE_TYPE1); // This call is actually superfluous

		assertTrue(prm.hasPendingRequests());

		prm.decreaseNumberOfPendingInstances(INSTANCE_TYPE2);
		prm.decreaseNumberOfPendingInstances(INSTANCE_TYPE2);
		prm.decreaseNumberOfPendingInstances(INSTANCE_TYPE2);
		prm.decreaseNumberOfPendingInstances(INSTANCE_TYPE2);

		assertFalse(prm.hasPendingRequests());
	}
}
