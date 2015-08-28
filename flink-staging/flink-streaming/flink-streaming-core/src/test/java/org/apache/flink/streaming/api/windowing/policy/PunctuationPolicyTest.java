/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.policy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.extractor.Extractor;
import org.apache.flink.streaming.api.windowing.extractor.FieldFromTuple;
import org.junit.Test;

public class PunctuationPolicyTest {

	// This value should not effect the policy. It is changed at each call to
	// verify this.
	private boolean triggered = false;

	@Test
	public void PunctuationTriggerTestWithoutExtraction() {
		PunctuationPolicy<Object, Object> policy = new PunctuationPolicy<Object, Object>(
				new TestObject(0));
		assertTrue("The present punctuation was not detected. (POS 1)",
				policy.notifyTrigger(new TestObject(0)));
		assertFalse("There was a punctuation detected which wasn't present. (POS 2)",
				policy.notifyTrigger(new TestObject(1)));
		policy.toString();
	}

	@Test
	public void PunctuationTriggerTestWithExtraction() {
		@SuppressWarnings({ "unchecked", "rawtypes" })
		PunctuationPolicy<Tuple2<Object, Object>, Object> policy = new PunctuationPolicy<Tuple2<Object, Object>, Object>(
				new TestObject(0), new FieldFromTuple(0));
		assertTrue("The present punctuation was not detected. (POS 3)",
				policy.notifyTrigger(new Tuple2<Object, Object>(new TestObject(0),
						new TestObject(1))));
		assertFalse("There was a punctuation detected which wasn't present. (POS 4)",
				policy.notifyTrigger(new Tuple2<Object, Object>(new TestObject(1),
						new TestObject(0))));
	}

	@Test
	public void PunctuationEvictionTestWithoutExtraction() {
		// The current buffer size should not effect the test. It's therefore
		// always 0 here.

		PunctuationPolicy<Object, Object> policy = new PunctuationPolicy<Object, Object>(
				new TestObject(0));
		assertEquals(
				"The present punctuation was not detected or the number of deleted tuples was wrong. (POS 5)",
				0, policy.notifyEviction(new TestObject(0), (triggered = !triggered), 0));
		for (int i = 0; i < 10; i++) {
			for (int j = 0; j < i; j++) {
				assertEquals("There was a punctuation detected which wasn't present. (POS 6)", 0,
						policy.notifyEviction(new TestObject(1), (triggered = !triggered), 0));
			}
			assertEquals(
					"The present punctuation was not detected or the number of deleted tuples was wrong. (POS 7)",
					i + 1, policy.notifyEviction(new TestObject(0), (triggered = !triggered), 0));
		}
	}

	@Test
	public void PunctuationEvictionTestWithExtraction() {
		// The current buffer size should not effect the test. It's therefore
		// always 0 here.

		@SuppressWarnings({ "unchecked", "rawtypes" })
		PunctuationPolicy<Tuple2<Object, Object>, Object> policy = new PunctuationPolicy<Tuple2<Object, Object>, Object>(
				new TestObject(0), new FieldFromTuple(0));
		assertEquals(
				"The present punctuation was not detected or the number of deleted tuples was wrong. (POS 10)",
				0, policy.notifyEviction(new Tuple2<Object, Object>(new TestObject(0),
						new TestObject(1)), (triggered = !triggered), 0));
		for (int i = 0; i < 10; i++) {
			for (int j = 0; j < i; j++) {
				assertEquals("There was a punctuation detected which wasn't present. (POS 9)", 0,
						policy.notifyEviction(new Tuple2<Object, Object>(new TestObject(1),
								new TestObject(0)), (triggered = !triggered), 0));
			}
			assertEquals(
					"The present punctuation was not detected or the number of deleted tuples was wrong. (POS 10)",
					i + 1, policy.notifyEviction(new Tuple2<Object, Object>(new TestObject(0),
							new TestObject(1)), (triggered = !triggered), 0));
		}
	}

	@Test
	public void testEquals() {
		Extractor<Integer, Integer> extractor = new Extractor<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer extract(Integer in) {
				return in;
			}
		};

		assertEquals(new PunctuationPolicy<Integer, Integer>(4),
				new PunctuationPolicy<Integer, Integer>(4));
		assertNotEquals(new PunctuationPolicy<Integer, Integer>(4),
				new PunctuationPolicy<Integer, Integer>(5));

		assertNotEquals(new PunctuationPolicy<Integer, Integer>(4, extractor),
				new PunctuationPolicy<Integer, Integer>(4));

		assertEquals(new PunctuationPolicy<Integer, Integer>(4, extractor),
				new PunctuationPolicy<Integer, Integer>(4, extractor));

		assertNotEquals(new PunctuationPolicy<Integer, Integer>(4),
				new PunctuationPolicy<Integer, Integer>(4, extractor));

	}

	private class TestObject {

		private int id;

		public TestObject(int id) {
			this.id = id;
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof TestObject && ((TestObject) o).getId() == this.id) {
				return true;
			} else {
				return false;
			}
		}

		public int getId() {
			return id;
		}
	}

}
