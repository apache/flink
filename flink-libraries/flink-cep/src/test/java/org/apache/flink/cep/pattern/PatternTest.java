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

package org.apache.flink.cep.pattern;

import org.apache.flink.cep.Event;
import org.apache.flink.cep.SubEvent;
import org.apache.flink.cep.pattern.conditions.OrCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.pattern.conditions.SubtypeCondition;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import static org.junit.Assert.*;

public class PatternTest extends TestLogger {
	/**
	 * These test simply test that the pattern construction completes without failure
	 */

	@Test
	public void testStrictContiguity() {
		Pattern<Object, ?> pattern = Pattern.begin("start").next("next").next("end");
		Pattern<Object, ?> previous;
		Pattern<Object, ?> previous2;

		assertNotNull(previous = pattern.getPrevious());
		assertNotNull(previous2 = previous.getPrevious());
		assertNull(previous2.getPrevious());

		assertEquals(pattern.getName(), "end");
		assertEquals(previous.getName(), "next");
		assertEquals(previous2.getName(), "start");
	}

	@Test
	public void testNonStrictContiguity() {
		Pattern<Object, ?> pattern = Pattern.begin("start").followedBy("next").followedBy("end");
		Pattern<Object, ?> previous;
		Pattern<Object, ?> previous2;

		assertNotNull(previous = pattern.getPrevious());
		assertNotNull(previous2 = previous.getPrevious());
		assertNull(previous2.getPrevious());

		assertTrue(pattern instanceof FollowedByPattern);
		assertTrue(previous instanceof FollowedByPattern);

		assertEquals(pattern.getName(), "end");
		assertEquals(previous.getName(), "next");
		assertEquals(previous2.getName(), "start");
	}

	@Test
	public void testStrictContiguityWithCondition() {
		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").next("next").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -7657256242101104925L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("foobar");
			}
		}).next("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -7597452389191504189L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getId() == 42;
			}
		});

		Pattern<Event, ?> previous;
		Pattern<Event, ?> previous2;

		assertNotNull(previous = pattern.getPrevious());
		assertNotNull(previous2 = previous.getPrevious());
		assertNull(previous2.getPrevious());

		assertNotNull(pattern.getCondition());
		assertNotNull(previous.getCondition());
		assertNull(previous2.getCondition());

		assertEquals(pattern.getName(), "end");
		assertEquals(previous.getName(), "next");
		assertEquals(previous2.getName(), "start");
	}

	@Test
	public void testPatternWithSubtyping() {
		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").next("subevent").subtype(SubEvent.class).followedBy("end");

		Pattern<Event, ?> previous;
		Pattern<Event, ?> previous2;

		assertNotNull(previous = pattern.getPrevious());
		assertNotNull(previous2 = previous.getPrevious());
		assertNull(previous2.getPrevious());

		assertNotNull(previous.getCondition());
		assertTrue(previous.getCondition() instanceof SubtypeCondition);

		assertEquals(pattern.getName(), "end");
		assertEquals(previous.getName(), "subevent");
		assertEquals(previous2.getName(), "start");
	}

	@Test
	public void testPatternWithSubtypingAndFilter() {
		Pattern<Event, Event> pattern = Pattern.<Event>begin("start").next("subevent").subtype(SubEvent.class).where(new SimpleCondition<SubEvent>() {
			private static final long serialVersionUID = -4118591291880230304L;

			@Override
			public boolean filter(SubEvent value) throws Exception {
				return false;
			}
		}).followedBy("end");

		Pattern<Event, ?> previous;
		Pattern<Event, ?> previous2;

		assertNotNull(previous = pattern.getPrevious());
		assertNotNull(previous2 = previous.getPrevious());
		assertNull(previous2.getPrevious());

		assertTrue(pattern instanceof FollowedByPattern);
		assertNotNull(previous.getCondition());

		assertEquals(pattern.getName(), "end");
		assertEquals(previous.getName(), "subevent");
		assertEquals(previous2.getName(), "start");
	}

	@Test
	public void testPatternWithOrFilter() {
		Pattern<Event, Event> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 3518061453394250543L;

			@Override
			public boolean filter(Event value) throws Exception {
				return false;
			}
		}).or(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 947463545810023841L;

			@Override
			public boolean filter(Event value) throws Exception {
				return false;
			}
		}).next("or").or(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -2775487887505922250L;

			@Override
			public boolean filter(Event value) throws Exception {
				return false;
			}
		}).followedBy("end");

		Pattern<Event, ?> previous;
		Pattern<Event, ?> previous2;

		assertNotNull(previous = pattern.getPrevious());
		assertNotNull(previous2 = previous.getPrevious());
		assertNull(previous2.getPrevious());

		assertTrue(pattern instanceof FollowedByPattern);
		assertFalse(previous.getCondition() instanceof OrCondition);
		assertTrue(previous2.getCondition() instanceof OrCondition);

		assertEquals(pattern.getName(), "end");
		assertEquals(previous.getName(), "or");
		assertEquals(previous2.getName(), "start");
	}

	@Test(expected = MalformedPatternException.class)
	public void testPatternCanHaveQuantifierSpecifiedOnce1() throws Exception {

		Pattern.begin("start").where(new SimpleCondition<Object>() {
			private static final long serialVersionUID = 8876425689668531458L;

			@Override
			public boolean filter(Object value) throws Exception {
				return true;
			}
		}).oneOrMore().zeroOrMore();
	}

	@Test(expected = MalformedPatternException.class)
	public void testPatternCanHaveQuantifierSpecifiedOnce2() throws Exception {

		Pattern.begin("start").where(new SimpleCondition<Object>() {
			private static final long serialVersionUID = 8311890695733430258L;

			@Override
			public boolean filter(Object value) throws Exception {
				return true;
			}
		}).zeroOrMore().times(1);
	}

	@Test(expected = MalformedPatternException.class)
	public void testPatternCanHaveQuantifierSpecifiedOnce3() throws Exception {

		Pattern.begin("start").where(new SimpleCondition<Object>() {
			private static final long serialVersionUID = 8093713196099078214L;

			@Override
			public boolean filter(Object value) throws Exception {
				return true;
			}
		}).times(1).oneOrMore();
	}

	@Test(expected = MalformedPatternException.class)
	public void testPatternCanHaveQuantifierSpecifiedOnce4() throws Exception {

		Pattern.begin("start").where(new SimpleCondition<Object>() {
			private static final long serialVersionUID = -2995187062849334113L;

			@Override
			public boolean filter(Object value) throws Exception {
				return true;
			}
		}).oneOrMore().oneOrMore(true);
	}

	@Test(expected = MalformedPatternException.class)
	public void testPatternCanHaveQuantifierSpecifiedOnce5() throws Exception {

		Pattern.begin("start").where(new SimpleCondition<Object>() {
			private static final long serialVersionUID = -2205071036073867531L;

			@Override
			public boolean filter(Object value) throws Exception {
				return true;
			}
		}).oneOrMore(true).zeroOrMore(true);
	}
}
