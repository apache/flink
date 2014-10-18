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

package org.apache.flink.runtime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.apache.flink.runtime.testutils.CommonTestUtils;

/**
 * This class contains tests for the {@link org.apache.flink.runtime.AbstractID} class.
 */
public class AbstractIDTest {
	/**
	 * Tests the serialization/deserialization of an abstract ID.
	 */
	@Test
	public void testSerialization() {
		final AbstractID origID = new AbstractID();
		try {
			final AbstractID copyID = (AbstractID) CommonTestUtils.createCopyWritable(origID);

			assertEquals(origID.hashCode(), copyID.hashCode());
			assertEquals(origID, copyID);

		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCompare() {
		try {
			AbstractID id1 = new AbstractID(0, 0);
			AbstractID id2 = new AbstractID(1, 0);
			AbstractID id3 = new AbstractID(0, 1);
			AbstractID id4 = new AbstractID(-1, 0);
			AbstractID id5 = new AbstractID(0, -1);
			AbstractID id6 = new AbstractID(-1, -1);
			
			AbstractID id7 = new AbstractID(Long.MAX_VALUE, Long.MAX_VALUE);
			AbstractID id8 = new AbstractID(Long.MIN_VALUE, Long.MIN_VALUE);
			AbstractID id9 = new AbstractID(Long.MAX_VALUE, Long.MIN_VALUE);
			AbstractID id10 = new AbstractID(Long.MIN_VALUE, Long.MAX_VALUE);
			
			// test self equality
			assertEquals(0, id1.compareTo(CommonTestUtils.createCopyWritable(id1)));
			assertEquals(0, id2.compareTo(CommonTestUtils.createCopyWritable(id2)));
			assertEquals(0, id3.compareTo(CommonTestUtils.createCopyWritable(id3)));
			assertEquals(0, id4.compareTo(CommonTestUtils.createCopyWritable(id4)));
			assertEquals(0, id5.compareTo(CommonTestUtils.createCopyWritable(id5)));
			assertEquals(0, id6.compareTo(CommonTestUtils.createCopyWritable(id6)));
			assertEquals(0, id7.compareTo(CommonTestUtils.createCopyWritable(id7)));
			assertEquals(0, id8.compareTo(CommonTestUtils.createCopyWritable(id8)));
			assertEquals(0, id9.compareTo(CommonTestUtils.createCopyWritable(id9)));
			assertEquals(0, id10.compareTo(CommonTestUtils.createCopyWritable(id10)));
			
			// test order
			assertCompare(id1, id2, -1);
			assertCompare(id1, id3, -1);
			assertCompare(id1, id4, 1);
			assertCompare(id1, id5, 1);
			assertCompare(id1, id6, 1);
			assertCompare(id2, id5, 1);
			assertCompare(id3, id5, 1);
			assertCompare(id2, id6, 1);
			assertCompare(id3, id6, 1);
			assertCompare(id1, id7, -1);
			assertCompare(id1, id8, 1);
			assertCompare(id7, id8, 1);
			assertCompare(id9, id10, -1);
			assertCompare(id7, id9, 1);
			assertCompare(id7, id10, 1);
			assertCompare(id8, id9, -1);
			assertCompare(id8, id10, -1);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	private static void assertCompare(AbstractID a, AbstractID b, int signum) {
		int cmpAB = a.compareTo(b);
		int cmpBA = b.compareTo(a);
		
		int sgnAB = cmpAB > 0 ? 1 : (cmpAB < 0 ? -1 : 0);
		int sgnBA = cmpBA > 0 ? 1 : (cmpBA < 0 ? -1 : 0);
		
		assertEquals(signum, sgnAB);
		assertTrue(sgnAB == -sgnBA);
	}
}
