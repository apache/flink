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
import org.apache.flink.cep.pattern.Quantifier.ConsumingStrategy;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.RichAndCondition;
import org.apache.flink.cep.pattern.conditions.RichOrCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.pattern.conditions.SubtypeCondition;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for constructing {@link Pattern}.
 */
public class PatternTest extends TestLogger {

	/**
	 * These test simply test that the pattern construction completes without failure.
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

		assertEquals(ConsumingStrategy.SKIP_TILL_NEXT, pattern.getQuantifier().getConsumingStrategy());
		assertEquals(ConsumingStrategy.SKIP_TILL_NEXT, previous.getQuantifier().getConsumingStrategy());

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
		assertNotNull(previous2.getCondition());

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

		assertEquals(ConsumingStrategy.SKIP_TILL_NEXT, pattern.getQuantifier().getConsumingStrategy());
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

		assertEquals(ConsumingStrategy.SKIP_TILL_NEXT, pattern.getQuantifier().getConsumingStrategy());
		assertFalse(previous.getCondition() instanceof RichOrCondition);
		assertTrue(previous2.getCondition() instanceof RichOrCondition);

		assertEquals(pattern.getName(), "end");
		assertEquals(previous.getName(), "or");
		assertEquals(previous2.getName(), "start");
	}

	@Test
	public void testRichCondition() {
		Pattern<Event, Event> pattern =
			Pattern.<Event>begin("start")
				.where(mock(IterativeCondition.class))
				.where(mock(IterativeCondition.class))
			.followedBy("end")
				.where(mock(IterativeCondition.class))
				.or(mock(IterativeCondition.class));
		assertTrue(pattern.getCondition() instanceof RichOrCondition);
		assertTrue(pattern.getPrevious().getCondition() instanceof RichAndCondition);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testPatternTimesNegativeTimes() throws Exception {
		Pattern.begin("start").where(dummyCondition()).times(-1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testPatternTimesNegativeFrom() throws Exception {
		Pattern.begin("start").where(dummyCondition()).times(-1, 2);
	}

	@Test(expected = MalformedPatternException.class)
	public void testPatternCanHaveQuantifierSpecifiedOnce1() throws Exception {

		Pattern.begin("start").where(dummyCondition()).oneOrMore().oneOrMore().optional();
	}

	@Test(expected = MalformedPatternException.class)
	public void testPatternCanHaveQuantifierSpecifiedOnce2() throws Exception {

		Pattern.begin("start").where(dummyCondition()).oneOrMore().optional().times(1);
	}

	@Test(expected = MalformedPatternException.class)
	public void testPatternCanHaveQuantifierSpecifiedOnce3() throws Exception {

		Pattern.begin("start").where(dummyCondition()).times(1).oneOrMore();
	}

	@Test(expected = MalformedPatternException.class)
	public void testPatternCanHaveQuantifierSpecifiedOnce4() throws Exception {

		Pattern.begin("start").where(dummyCondition()).oneOrMore().oneOrMore();
	}

	@Test(expected = MalformedPatternException.class)
	public void testPatternCanHaveQuantifierSpecifiedOnce5() throws Exception {

		Pattern.begin("start").where(dummyCondition()).oneOrMore().oneOrMore().optional();
	}

	@Test(expected = MalformedPatternException.class)
	public void testNotNextCannotBeOneOrMore() throws Exception {
		Pattern.begin("start").where(dummyCondition()).notNext("not").where(dummyCondition()).oneOrMore();
	}

	@Test(expected = MalformedPatternException.class)
	public void testNotNextCannotBeTimes() throws Exception {
		Pattern.begin("start").where(dummyCondition()).notNext("not").where(dummyCondition()).times(3);
	}

	@Test(expected = MalformedPatternException.class)
	public void testNotNextCannotBeOptional() throws Exception {

		Pattern.begin("start").where(dummyCondition()).notNext("not").where(dummyCondition()).optional();
	}

	@Test(expected = MalformedPatternException.class)
	public void testNotFollowedCannotBeOneOrMore() throws Exception {
		Pattern.begin("start").where(dummyCondition()).notFollowedBy("not").where(dummyCondition()).oneOrMore();
	}

	@Test(expected = MalformedPatternException.class)
	public void testNotFollowedCannotBeTimes() throws Exception {
		Pattern.begin("start").where(dummyCondition()).notFollowedBy("not").where(dummyCondition()).times(3);
	}

	@Test(expected = MalformedPatternException.class)
	public void testNotFollowedCannotBeOptional() throws Exception {

		Pattern.begin("start").where(dummyCondition()).notFollowedBy("not").where(dummyCondition()).optional();
	}

	@Test(expected = MalformedPatternException.class)
	public void testUntilCannotBeAppliedToTimes() throws Exception {

		Pattern.begin("start").where(dummyCondition()).times(1).until(dummyCondition());
	}

	@Test(expected = MalformedPatternException.class)
	public void testUntilCannotBeAppliedToSingleton() throws Exception {

		Pattern.begin("start").where(dummyCondition()).until(dummyCondition());
	}

	@Test(expected = MalformedPatternException.class)
	public void testUntilCannotBeAppliedTwice() throws Exception {

		Pattern.begin("start").where(dummyCondition()).until(dummyCondition()).until(dummyCondition());
	}

	private SimpleCondition<Object> dummyCondition() {
		return new SimpleCondition<Object>() {
			private static final long serialVersionUID = -2205071036073867531L;

			@Override
			public boolean filter(Object value) throws Exception {
				return true;
			}
		};
	}
}
