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

package org.apache.flink.graph.types;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OptionalBooleanTest {

	@Test
	public void testIsMismatchedWith()
			throws Exception {
		OptionalBoolean u = new OptionalBoolean(true);
		OptionalBoolean t = new OptionalBoolean(true);
		OptionalBoolean f = new OptionalBoolean(true);

		t.set(true);
		f.set(false);

		// true, true
		assertFalse(t.isMismatchedWith(t));

		// true, false
		assertTrue(t.isMismatchedWith(f));

		// true, unknown
		assertFalse(t.isMismatchedWith(u));

		// false, true
		assertTrue(f.isMismatchedWith(t));

		// false, false
		assertFalse(f.isMismatchedWith(f));

		// false, unknown
		assertFalse(f.isMismatchedWith(u));

		// unknown, true
		assertFalse(u.isMismatchedWith(t));

		// unknown, false
		assertFalse(u.isMismatchedWith(f));

		// unknown, unknown
		assertFalse(u.isMismatchedWith(u));
	}

	@Test
	public void testMergeWith()
			throws Exception {
		OptionalBoolean u_t = new OptionalBoolean(true);
		OptionalBoolean u_f = new OptionalBoolean(false);
		OptionalBoolean t_t = new OptionalBoolean(true);
		OptionalBoolean t_f = new OptionalBoolean(false);
		OptionalBoolean f_t = new OptionalBoolean(true);
		OptionalBoolean f_f = new OptionalBoolean(false);

		t_t.set(true);
		t_f.set(true);
		f_t.set(false);
		f_f.set(false);

		// true, true => true
		t_t.mergeWith(t_t);
		assertTrue(t_t.get());

		// true, false => default false
		t_f.mergeWith(f_f);
		assertFalse(t_f.get());
		t_f.set(true);

		// true, false => default true
		t_t.mergeWith(f_f);
		assertTrue(t_t.get());
		t_t.set(true);

		// true, unknown => true
		t_t.mergeWith(u_f);
		assertTrue(t_t.get());

		// false, true => default false
		f_f.mergeWith(t_t);
		assertFalse(f_f.get());
		f_f.set(false);

		// false, true => default true
		f_t.mergeWith(t_t);
		assertTrue(f_t.get());
		f_t.set(false);

		// false, false => false
		f_f.mergeWith(f_f);
		assertFalse(f_f.get());

		// false, unknown => false
		f_f.mergeWith(u_t);
		assertFalse(f_f.get());

		// unknown, true => true
		u_t.mergeWith(t_f);
		assertTrue(u_t.get());
		u_t.unset();

		// unknown, false => false
		u_f.mergeWith(f_t);
		assertFalse(u_f.get());
		u_f.unset();

		// unknown, unknown => default true
		u_t.mergeWith(u_f);
		assertTrue(u_t.get());
		u_t.unset();

		// unknown, unknown => default false
		u_f.mergeWith(u_t);
		assertFalse(u_f.get());
		u_f.unset();
	}
}
