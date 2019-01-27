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

package org.apache.flink.core.io;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.io.LocatableInputSplitAssigner;

import org.junit.Test;


public class LocatableSplitAssignerTest {
	
	@Test
	public void testSerialSplitAssignmentWithNullHost() {
		try {
			final int NUM_SPLITS = 50;
			final String[][] hosts = new String[][] {
					new String[] { "localhost" },
					new String[0],
					null
			};
			
			// load some splits
			List<LocatableInputSplit> splits = new LinkedList<LocatableInputSplit>();
			for (int i = 0; i < NUM_SPLITS; i++) {
				splits.add(new LocatableInputSplit(i, hosts[i%3]));
			}
			
			// get all available splits
			LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);
			InputSplit is = null;
			while ((is = ia.getNextInputSplit(null, 0)) != null) {
				assertTrue(splits.get(0) == is);
				splits.remove(is);
			}
			
			// check we had all
			assertTrue(splits.isEmpty());
			assertNull(ia.getNextInputSplit("", 0));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSerialSplitAssignmentAllForSameHost() {
		try {
			final int NUM_SPLITS = 50;
			
			// load some splits
			List<LocatableInputSplit> splits = new LinkedList<>();
			for (int i = 0; i < NUM_SPLITS; i++) {
				splits.add(new LocatableInputSplit(i, "testhost"));
			}
			
			// get all available splits
			LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);
			InputSplit is = null;
			while ((is = ia.getNextInputSplit("testhost", 0)) != null) {
				assertTrue(splits.get(0) == is);
				splits.remove(is);
			}
			
			// check we had all
			assertTrue(splits.isEmpty());
			assertNull(ia.getNextInputSplit("", 0));
			
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSerialSplitAssignmentAllForRemoteHost() {
		try {
			final String[] hosts = { "host1", "host1", "host1", "host2", "host2", "host3" };
			final int NUM_SPLITS = 10 * hosts.length;
			
			// load some splits
			List<LocatableInputSplit> splits = new LinkedList<>();
			for (int i = 0; i < NUM_SPLITS; i++) {
				splits.add(new LocatableInputSplit(i, hosts[i % hosts.length]));
			}
			
			// get all available splits
			LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);
			InputSplit is = null;
			while ((is = ia.getNextInputSplit("testhost", 0)) != null) {
				assertTrue(splits.get(0) == is);
				splits.remove(is);
			}
			
			// check we had all
			assertTrue(splits.isEmpty());
			assertNull(ia.getNextInputSplit("anotherHost", 0));
			
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSerialSplitAssignmentForMixedHosts() {
		try {
			final String[] hosts = { "host1", "host2", "host3" };
			List<LocatableInputSplit> splits = new LinkedList<>();
			splits.add(new LocatableInputSplit(0, new String[]{hosts[0], hosts[1], hosts[2]}));
			splits.add(new LocatableInputSplit(1, new String[]{hosts[1], hosts[2], hosts[0]}));
			splits.add(new LocatableInputSplit(2, new String[]{hosts[2], hosts[0], hosts[1]}));
			splits.add(new LocatableInputSplit(3, new String[]{hosts[2], hosts[1], hosts[0]}));
			splits.add(new LocatableInputSplit(4, new String[]{hosts[1], hosts[0], hosts[2]}));
			splits.add(new LocatableInputSplit(5, new String[]{hosts[0], hosts[2], hosts[1]}));

			LocatableInputSplitAssigner ia = new LocatableInputSplitAssigner(splits);
			assertEquals(2, ia.getNextInputSplit(hosts[2], 0).getSplitNumber());
			assertEquals(1, ia.getNextInputSplit(hosts[1], 0).getSplitNumber());
			assertEquals(0, ia.getNextInputSplit(hosts[0], 0).getSplitNumber());
			assertEquals(4, ia.getNextInputSplit(hosts[1], 0).getSplitNumber());
			assertEquals(5, ia.getNextInputSplit(hosts[0], 0).getSplitNumber());
			assertEquals(3, ia.getNextInputSplit(hosts[2], 0).getSplitNumber());

			assertEquals(null, ia.getNextInputSplit(hosts[2], 0));

			LocatableInputSplitAssigner ia2 = new LocatableInputSplitAssigner(splits);
			assertEquals(1, ia2.getNextInputSplit(hosts[1], 0).getSplitNumber());
			assertEquals(4, ia2.getNextInputSplit(hosts[1], 0).getSplitNumber());
			assertEquals(0, ia2.getNextInputSplit(hosts[0], 0).getSplitNumber());
			assertEquals(5, ia2.getNextInputSplit(hosts[0], 0).getSplitNumber());
			assertEquals(2, ia2.getNextInputSplit(hosts[2], 0).getSplitNumber());
			assertEquals(3, ia2.getNextInputSplit(hosts[2], 0).getSplitNumber());

			LocatableInputSplitAssigner ia3 = new LocatableInputSplitAssigner(splits);
			assertEquals(0, ia3.getNextInputSplit(hosts[0], 0).getSplitNumber());
			assertEquals(2, ia3.getNextInputSplit(hosts[2], 0).getSplitNumber());
			assertEquals(1, ia3.getNextInputSplit(hosts[1], 0).getSplitNumber());
			assertEquals(4, ia3.getNextInputSplit(hosts[1], 0).getSplitNumber());
			assertEquals(3, ia3.getNextInputSplit(hosts[1], 0).getSplitNumber());
			assertEquals(5, ia3.getNextInputSplit(hosts[1], 0).getSplitNumber());

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
