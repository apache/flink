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

package org.apache.flink.api.common.operators;

import static org.junit.Assert.*;

import org.apache.flink.api.common.operators.SemanticProperties.InvalidSemanticAnnotationException;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.junit.Test;

public class SingleInputSemanticPropertiesTest {

	@Test
	public void testGetTargetFields() {

		SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
		sp.addForwardedField(0,1);
		sp.addForwardedField(1,4);
		sp.addForwardedField(2,3);
		sp.addForwardedField(3,2);

		assertEquals(1, sp.getForwardingTargetFields(0, 0).size());
		assertEquals(1, sp.getForwardingTargetFields(0, 1).size());
		assertEquals(1, sp.getForwardingTargetFields(0, 2).size());
		assertEquals(1, sp.getForwardingTargetFields(0, 3).size());
		assertTrue(sp.getForwardingTargetFields(0, 0).contains(1));
		assertTrue(sp.getForwardingTargetFields(0, 1).contains(4));
		assertTrue(sp.getForwardingTargetFields(0, 2).contains(3));
		assertTrue(sp.getForwardingTargetFields(0, 3).contains(2));
		assertNotNull(sp.getForwardingTargetFields(0, 4));
		assertEquals(0, sp.getForwardingTargetFields(0, 4).size());

		sp = new SingleInputSemanticProperties();
		sp.addForwardedField(0,0);
		sp.addForwardedField(0,4);
		sp.addForwardedField(1,1);
		sp.addForwardedField(1,2);
		sp.addForwardedField(1,3);

		assertEquals(2, sp.getForwardingTargetFields(0, 0).size());
		assertEquals(3, sp.getForwardingTargetFields(0, 1).size());
		assertTrue(sp.getForwardingTargetFields(0, 0).contains(0));
		assertTrue(sp.getForwardingTargetFields(0, 0).contains(4));
		assertTrue(sp.getForwardingTargetFields(0, 1).contains(1));
		assertTrue(sp.getForwardingTargetFields(0, 1).contains(2));
		assertTrue(sp.getForwardingTargetFields(0, 1).contains(3));
		assertNotNull(sp.getForwardingTargetFields(0, 2));
		assertEquals(0, sp.getForwardingTargetFields(0, 2).size());

	}

	@Test
	public void testGetSourceField() {

		SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
		sp.addForwardedField(0,1);
		sp.addForwardedField(1,4);
		sp.addForwardedField(2,3);
		sp.addForwardedField(3,2);

		assertEquals(0, sp.getForwardingSourceField(0, 1));
		assertEquals(1, sp.getForwardingSourceField(0, 4));
		assertEquals(2, sp.getForwardingSourceField(0, 3));
		assertEquals(3, sp.getForwardingSourceField(0, 2));
		assertTrue(sp.getForwardingSourceField(0, 0) < 0);
		assertTrue(sp.getForwardingSourceField(0, 5) < 0);

		sp = new SingleInputSemanticProperties();
		sp.addForwardedField(0,0);
		sp.addForwardedField(0,4);
		sp.addForwardedField(1,1);
		sp.addForwardedField(1,2);
		sp.addForwardedField(1,3);

		assertEquals(0, sp.getForwardingSourceField(0, 0));
		assertEquals(0, sp.getForwardingSourceField(0, 4));
		assertEquals(1, sp.getForwardingSourceField(0, 1));
		assertEquals(1, sp.getForwardingSourceField(0, 2));
		assertEquals(1, sp.getForwardingSourceField(0, 3));
		assertTrue(sp.getForwardingSourceField(0, 5) < 0);

	}

	@Test
	public void testGetReadSet() {

		SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
		sp.addReadFields(new FieldSet(0, 1));

		assertEquals(2, sp.getReadFields(0).size());
		assertTrue(sp.getReadFields(0).contains(0));
		assertTrue(sp.getReadFields(0).contains(1));

		sp.addReadFields(new FieldSet(3));

		assertEquals(3, sp.getReadFields(0).size());
		assertTrue(sp.getReadFields(0).contains(0));
		assertTrue(sp.getReadFields(0).contains(1));
		assertTrue(sp.getReadFields(0).contains(3));
	}

	@Test(expected = InvalidSemanticAnnotationException.class)
	public void testAddForwardedFieldsTargetTwice() {

		SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
		sp.addForwardedField(0, 2);
		sp.addForwardedField(1, 2);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testGetTargetFieldInvalidIndex() {

		SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
		sp.addForwardedField(0,0);

		sp.getForwardingTargetFields(1, 0);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testGetSourceFieldInvalidIndex() {

		SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
		sp.addForwardedField(0,0);

		sp.getForwardingSourceField(1, 0);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testGetReadFieldsInvalidIndex() {

		SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
		sp.addReadFields(new FieldSet(0, 1));

		sp.getReadFields(1);
	}

	@Test
	public void testAllForwardedSingleInputSemProps() {

		SingleInputSemanticProperties sp = new SingleInputSemanticProperties.AllFieldsForwardedProperties();

		assertEquals(1, sp.getForwardingTargetFields(0, 0).size());
		assertEquals(1, sp.getForwardingTargetFields(0, 1).size());
		assertEquals(1, sp.getForwardingTargetFields(0, 123).size());
		assertTrue(sp.getForwardingTargetFields(0, 0).contains(0));
		assertTrue(sp.getForwardingTargetFields(0, 1).contains(1));
		assertTrue(sp.getForwardingTargetFields(0, 123).contains(123));

		assertEquals(0, sp.getForwardingSourceField(0, 0));
		assertEquals(2, sp.getForwardingSourceField(0, 2));
		assertEquals(123, sp.getForwardingSourceField(0, 123));
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testAllForwardedSingleInputSemPropsInvalidIndex1() {

		SingleInputSemanticProperties sp = new SingleInputSemanticProperties.AllFieldsForwardedProperties();
		sp.getForwardingSourceField(1, 0);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testAllForwardedSingleInputSemPropsInvalidIndex2() {

		SingleInputSemanticProperties sp = new SingleInputSemanticProperties.AllFieldsForwardedProperties();
		sp.getForwardingTargetFields(1, 0);
	}
}
