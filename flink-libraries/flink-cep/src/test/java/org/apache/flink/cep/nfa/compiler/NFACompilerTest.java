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

package org.apache.flink.cep.nfa.compiler;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.SubEvent;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.State;
import org.apache.flink.cep.nfa.StateTransition;
import org.apache.flink.cep.nfa.StateTransitionAction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.MalformedPatternException;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cep.utils.NFAUtils.compile;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link NFACompiler}.
 */
public class NFACompilerTest extends TestLogger {

	private static final SimpleCondition<Event> startFilter = new SimpleCondition<Event>() {
		private static final long serialVersionUID = 3314714776170474221L;

		@Override
		public boolean filter(Event value) throws Exception {
			return value.getPrice() > 2;
		}
	};

	private static final SimpleCondition<Event> endFilter = new SimpleCondition<Event>() {
		private static final long serialVersionUID = 3990995859716364087L;

		@Override
		public boolean filter(Event value) throws Exception {
			return value.getName().equals("end");
		}
	};

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void testNFACompilerUniquePatternName() {

		// adjust the rule
		expectedException.expect(MalformedPatternException.class);
		expectedException.expectMessage("Duplicate pattern name: start. Names must be unique.");

		Pattern<Event, ?> invalidPattern = Pattern.<Event>begin("start").where(new TestFilter())
			.followedBy("middle").where(new TestFilter())
			.followedBy("start").where(new TestFilter());

		// here we must have an exception because of the two "start" patterns with the same name.
		compile(invalidPattern, false);
	}

	@Test
	public void testNFACompilerPatternEndsWithNotFollowedBy() {

		// adjust the rule
		expectedException.expect(MalformedPatternException.class);
		expectedException.expectMessage("NotFollowedBy is not supported as a last part of a Pattern!");

		Pattern<Event, ?> invalidPattern = Pattern.<Event>begin("start").where(new TestFilter())
			.followedBy("middle").where(new TestFilter())
			.notFollowedBy("end").where(new TestFilter());

		// here we must have an exception because of the two "start" patterns with the same name.
		compile(invalidPattern, false);
	}

	/**
	 * A filter implementation to test invalid pattern specification with
	 * duplicate pattern names. Check {@link #testNFACompilerUniquePatternName()}.
	 */
	private static class TestFilter extends SimpleCondition<Event> {

		private static final long serialVersionUID = -3863103355752267133L;

		@Override
		public boolean filter(Event value) throws Exception {
			throw new RuntimeException("It should never arrive here.");
		}
	}

	/**
	 * Tests that the NFACompiler generates the correct NFA from a given Pattern.
	 */
	@Test
	public void testNFACompilerWithSimplePattern() {
		Pattern<Event, Event> pattern = Pattern.<Event>begin("start").where(startFilter)
			.followedBy("middle").subtype(SubEvent.class)
			.next("end").where(endFilter);

		NFA<Event> nfa = compile(pattern, false);

		Collection<State<Event>> states = nfa.getStates();
		assertEquals(4, states.size());

		Map<String, State<Event>> stateMap = new HashMap<>();
		for (State<Event> state : states) {
			stateMap.put(state.getName(), state);
		}

		assertTrue(stateMap.containsKey("start"));
		State<Event> startState = stateMap.get("start");
		assertTrue(startState.isStart());
		final Set<Tuple2<String, StateTransitionAction>> startTransitions = unfoldTransitions(startState);
		assertEquals(Sets.newHashSet(
			Tuple2.of("middle", StateTransitionAction.TAKE)
		), startTransitions);

		assertTrue(stateMap.containsKey("middle"));
		State<Event> middleState = stateMap.get("middle");
		final Set<Tuple2<String, StateTransitionAction>> middleTransitions = unfoldTransitions(middleState);
		assertEquals(Sets.newHashSet(
			Tuple2.of("middle", StateTransitionAction.IGNORE),
			Tuple2.of("end", StateTransitionAction.TAKE)
		), middleTransitions);

		assertTrue(stateMap.containsKey("end"));
		State<Event> endState = stateMap.get("end");
		final Set<Tuple2<String, StateTransitionAction>> endTransitions = unfoldTransitions(endState);
		assertEquals(Sets.newHashSet(
			Tuple2.of(NFACompiler.ENDING_STATE_NAME, StateTransitionAction.TAKE)
		), endTransitions);

		assertTrue(stateMap.containsKey(NFACompiler.ENDING_STATE_NAME));
		State<Event> endingState = stateMap.get(NFACompiler.ENDING_STATE_NAME);
		assertTrue(endingState.isFinal());
		assertEquals(0, endingState.getStateTransitions().size());
	}

	@Test
	public void testNoUnnecessaryStateCopiesCreated() {
		final Pattern<Event, Event> pattern = Pattern.<Event>begin("start").where(startFilter)
			.notFollowedBy("not").where(startFilter)
			.followedBy("oneOrMore").where(startFilter).oneOrMore()
			.followedBy("end").where(endFilter);

		final NFACompiler.NFAFactoryCompiler<Event> nfaFactoryCompiler = new NFACompiler.NFAFactoryCompiler<>(pattern);
		nfaFactoryCompiler.compileFactory();

		int endStateCount = 0;
		for (State<Event> state : nfaFactoryCompiler.getStates()) {
			if (state.getName().equals("end")) {
				endStateCount++;
			}
		}

		assertEquals(1, endStateCount);
	}

	@Test
	public void testSkipToNotExistsMatchingPattern() {
		expectedException.expect(MalformedPatternException.class);
		expectedException.expectMessage("The pattern name specified in AfterMatchSkipStrategy can not be found in the given Pattern");

		Pattern<Event, ?> invalidPattern = Pattern.<Event>begin("start",
			AfterMatchSkipStrategy.skipToLast("midd")).where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().contains("a");
			}
		}).next("middle").where(
			new SimpleCondition<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().contains("d");
				}
			}
		).oneOrMore().optional().next("end").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().contains("c");
			}
		});

		compile(invalidPattern, false);
	}

	private <T> Set<Tuple2<String, StateTransitionAction>> unfoldTransitions(final State<T> state) {
		final Set<Tuple2<String, StateTransitionAction>> transitions = new HashSet<>();
		for (StateTransition<T> transition : state.getStateTransitions()) {
			transitions.add(Tuple2.of(
				transition.getTargetState().getName(),
				transition.getAction()));
		}
		return transitions;
	}

	@Test
	public void testCheckingEmptyMatches() {
		assertThat(NFACompiler.canProduceEmptyMatches(Pattern.begin("a").optional()), is(true));
		assertThat(NFACompiler.canProduceEmptyMatches(Pattern.begin("a").oneOrMore().optional()), is(true));
		assertThat(NFACompiler.canProduceEmptyMatches(Pattern.begin("a").oneOrMore().optional().next("b").optional()), is(true));

		assertThat(NFACompiler.canProduceEmptyMatches(Pattern.begin("a")), is(false));
		assertThat(NFACompiler.canProduceEmptyMatches(Pattern.begin("a").oneOrMore()), is(false));
		assertThat(NFACompiler.canProduceEmptyMatches(Pattern.begin("a").oneOrMore().next("b").optional()), is(false));
	}
}
