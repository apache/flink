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

package org.apache.flink.graph.utils.proxy;

import org.apache.flink.graph.utils.proxy.OptionalBoolean.State;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link OptionalBoolean}.
 */
public class OptionalBooleanTest {

	private OptionalBoolean u;
	private OptionalBoolean f;
	private OptionalBoolean t;
	private OptionalBoolean c;

	@Before
	public void setup() {
		u = new OptionalBoolean(false, true);
		f = new OptionalBoolean(false, true);
		t = new OptionalBoolean(false, true);
		c = new OptionalBoolean(false, true);

		f.set(false);
		t.set(true);

		c.set(true);
		c.mergeWith(f);
	}

	@Test
	public void testIsMismatchedWith()
			throws Exception {
		// unset, unset
		assertFalse(u.conflictsWith(u));

		// unset, false
		assertFalse(u.conflictsWith(f));

		// unset, true
		assertFalse(u.conflictsWith(t));

		// unset, conflicting
		assertTrue(u.conflictsWith(c));

		// false, unset
		assertFalse(f.conflictsWith(u));

		// false, false
		assertFalse(f.conflictsWith(f));

		// false, true
		assertTrue(f.conflictsWith(t));

		// false, conflicting
		assertTrue(f.conflictsWith(c));

		// true, unset
		assertFalse(t.conflictsWith(u));

		// true, false
		assertTrue(t.conflictsWith(f));

		// true, true
		assertFalse(t.conflictsWith(t));

		// true, conflicting
		assertTrue(t.conflictsWith(c));

		// conflicting, unset
		assertTrue(c.conflictsWith(u));

		// conflicting, false
		assertTrue(c.conflictsWith(f));

		// conflicting, true
		assertTrue(c.conflictsWith(t));

		// conflicting, conflicting
		assertTrue(c.conflictsWith(c));
	}

	@Test
	public void testMergeWith()
			throws Exception {
		// unset, unset => unset
		u.mergeWith(u);
		assertEquals(State.UNSET, u.getState());

		// unset, false => false
		u.mergeWith(f);
		assertEquals(State.FALSE, u.getState());
		u.unset();

		// unset, true => true
		u.mergeWith(t);
		assertEquals(State.TRUE, u.getState());
		u.unset();

		// unset, conflicting => conflicting
		u.mergeWith(c);
		assertEquals(State.CONFLICTING, u.getState());
		u.unset();

		// false, unset => false
		f.mergeWith(u);
		assertEquals(State.FALSE, f.getState());

		// false, false => false
		f.mergeWith(f);
		assertEquals(State.FALSE, f.getState());

		// false, true => conflicting
		f.mergeWith(t);
		assertEquals(State.CONFLICTING, f.getState());
		f.set(false);

		// false, conflicting => conflicting
		f.mergeWith(c);
		assertEquals(State.CONFLICTING, f.getState());
		f.set(false);

		// true, unset => true
		t.mergeWith(u);
		assertEquals(State.TRUE, t.getState());

		// true, false => conflicting
		t.mergeWith(f);
		assertEquals(State.CONFLICTING, t.getState());
		t.set(true);

		// true, true => true
		t.mergeWith(t);
		assertEquals(State.TRUE, t.getState());

		// true, conflicting => conflicting
		t.mergeWith(c);
		assertEquals(State.CONFLICTING, t.getState());
		t.set(true);

		// conflicting, unset => conflicting
		c.mergeWith(u);
		assertEquals(State.CONFLICTING, c.getState());

		// conflicting, false => conflicting
		c.mergeWith(f);
		assertEquals(State.CONFLICTING, c.getState());

		// conflicting, true => conflicting
		c.mergeWith(t);
		assertEquals(State.CONFLICTING, c.getState());

		// conflicting, conflicting => conflicting
		c.mergeWith(c);
		assertEquals(State.CONFLICTING, c.getState());
	}
}
