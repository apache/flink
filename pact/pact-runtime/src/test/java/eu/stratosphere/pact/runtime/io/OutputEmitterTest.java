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

package eu.stratosphere.pact.runtime.io;

import junit.framework.TestCase;

import org.junit.Test;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

public class OutputEmitterTest extends TestCase {

	@Test
	public static void testPartitioning() {

		@SuppressWarnings("unchecked")
		OutputEmitter oe1 = new OutputEmitter(ShipStrategy.PARTITION_HASH, new JobID(), new int[] {0}, new Class[] {PactInteger.class});

		int[] hit = new int[100];

		for (int i = 0; i < 1000000; i++) {
			PactInteger k = new PactInteger(i);
			PactRecord rec = new PactRecord(k);
			
			hit[oe1.selectChannels(rec, hit.length)[0]]++;

		}

		for (int i = 0; i < hit.length; i++) {
			assertTrue(hit[i] > 0);
		}

		@SuppressWarnings("unchecked")
		OutputEmitter oe2 = new OutputEmitter(ShipStrategy.PARTITION_HASH, new JobID(), new int[] {0}, new Class[] {PactInteger.class});

		hit = new int[10];

		for (int i = 0; i < 1000; i++) {
			PactString k = new PactString(i + "");
			PactRecord rec = new PactRecord(k);
				
			hit[oe2.selectChannels(rec, hit.length)[0]]++;

		}

		for (int i = 0; i < hit.length; i++) {
			assertTrue(hit[i] > 0);
		}
	}
}
