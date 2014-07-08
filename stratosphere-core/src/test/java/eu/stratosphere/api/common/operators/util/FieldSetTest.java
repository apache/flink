/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/

package eu.stratosphere.api.common.operators.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

public class FieldSetTest {
	
	@Test
	public void testFieldSetConstructors() {
		check(new FieldSet());
		check(FieldSet.EMPTY_SET);
		check(new FieldSet(14), 14);
		check(new FieldSet(new Integer(3)), 3);
		check(new FieldSet(7, 4, 1), 1, 4, 7);
		check(new FieldSet(7, 4, 1, 4, 7, 1, 4, 2), 1, 4, 2, 7);
	}
	
	@Test
	public void testFieldSetAdds() {
		check(new FieldSet().addField(1).addField(2), 1, 2);
		check(FieldSet.EMPTY_SET.addField(3).addField(2), 3, 2);
		check(new FieldSet(13).addFields(new FieldSet(17, 31, 42)), 17, 13, 42, 31);
		check(new FieldSet(14).addFields(new FieldSet(17)), 17, 14);
		check(new FieldSet(3).addFields(2, 8, 5, 7), 3, 2, 8, 5, 7);
		check(new FieldSet().addFields(new FieldSet()));
		check(new FieldSet().addFields(new FieldSet(3, 4)), 4, 3);
		check(new FieldSet(5, 1).addFields(new FieldSet()), 5, 1);
	}
	
	@Test
	public void testImmutability() {
		FieldSet s1 = new FieldSet();
		FieldSet s2 = new FieldSet(5);
		FieldSet s3 = new FieldSet(new Integer(7));
		FieldSet s4 = new FieldSet(5, 4, 7, 6);
		
		s1.addFields(s2).addFields(s3);
		s2.addFields(s4);
		s4.addFields(s1);
		
		s1.addField(new Integer(14));
		s2.addFields(78, 13, 66, 3);
		
		assertEquals(0, s1.size());
		assertEquals(1, s2.size());
		assertEquals(1, s3.size());
		assertEquals(4, s4.size());
	}
	
	@Test
	public void testAddListToSet() {
		check(new FieldSet().addField(1).addFields(new FieldList(14, 3, 1)), 1, 3, 14);
	}
	
	private static void check(FieldSet set, int... elements) {
		if (elements == null) {
			assertEquals(0, set.size());
			return;
		}
		
		assertEquals(elements.length, set.size());
		
		// test contains
		for (int i : elements) {
			set.contains(i);
		}
		
		Arrays.sort(elements);
		
		// test to array
		{
			int[] arr = set.toArray();
			Arrays.sort(arr);
			assertTrue(Arrays.equals(arr, elements));
		}
		
		{
			int[] fromIter = new int[set.size()];
			Iterator<Integer> iter = set.iterator();
			
			for (int i = 0; i < fromIter.length; i++) {
				fromIter[i] = iter.next();
			}
			assertFalse(iter.hasNext());
			Arrays.sort(fromIter);
			assertTrue(Arrays.equals(fromIter, elements));
		}
	}
}
