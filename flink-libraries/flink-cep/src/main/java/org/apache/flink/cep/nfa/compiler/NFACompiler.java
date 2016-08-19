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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.State;
import org.apache.flink.cep.nfa.StateTransition;
import org.apache.flink.cep.nfa.StateTransitionAction;
import org.apache.flink.cep.pattern.FollowedByPattern;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Compiler class containing methods to compile a {@link Pattern} into a {@link NFA} or a
 * {@link NFAFactory}.
 */
public class NFACompiler {

	protected final static String BEGINNING_STATE_NAME = "$beginningState$";

	/**
	 * Compiles the given pattern into a {@link NFA}.
	 *
	 * @param pattern Definition of sequence pattern
	 * @param inputTypeSerializer Serializer for the input type
	 * @param timeoutHandling True if the NFA shall return timed out event patterns
	 * @param <T> Type of the input events
	 * @return Non-deterministic finite automaton representing the given pattern
	 */
	public static <T> NFA<T> compile(
		Pattern<T, ?> pattern,
		TypeSerializer<T> inputTypeSerializer,
		boolean timeoutHandling) {
		NFAFactory<T> factory = compileFactory(pattern, inputTypeSerializer, timeoutHandling);

		return factory.createNFA();
	}

	/**
	 * Compiles the given pattern into a {@link NFAFactory}. The NFA factory can be used to create
	 * multiple NFAs.
	 *
	 * @param pattern Definition of sequence pattern
	 * @param inputTypeSerializer Serializer for the input type
	 * @param timeoutHandling True if the NFA shall return timed out event patterns
	 * @param <T> Type of the input events
	 * @return Factory for NFAs corresponding to the given pattern
	 */
	@SuppressWarnings("unchecked")
	public static <T> NFAFactory<T> compileFactory(
		Pattern<T, ?> pattern,
		TypeSerializer<T> inputTypeSerializer,
		boolean timeoutHandling) {
		if (pattern == null) {
			// return a factory for empty NFAs
			return new NFAFactoryImpl<T>(inputTypeSerializer, 0, Collections.<State<T>>emptyList(), timeoutHandling);
		} else {
			ArrayList<Pattern<T, ?>> patterns = createPatternsList(pattern);
			// set of all generated states
			Map<String, State<T>> states = createStatesFrom(patterns);

			long windowTime = pattern.getWindowTime() != null ? pattern.getWindowTime().toMilliseconds() : 0L;

			// we're traversing the pattern from the end to the beginning --> the first state is the final state
			for (int i = patterns.size() - 2; i >= 0; i--) {
				Pattern<T, ?> succeedingPattern = patterns.get(i + 1);
				Pattern<T, ?> currentPattern = patterns.get(i);
				State<T> currentState = states.get(currentPattern.getName());

				Time currentWindowTime = currentPattern.getWindowTime();

				if (currentWindowTime != null && currentWindowTime.toMilliseconds() < windowTime) {
					// the window time is the global minimum of all window times of each state
					windowTime = currentWindowTime.toMilliseconds();
				}

				addTransitions(currentState, i, patterns, states);

				if (succeedingPattern instanceof FollowedByPattern) {
					// the followed by pattern entails a reflexive ignore transition
					currentState.addStateTransition(new StateTransition<T>(
						StateTransitionAction.IGNORE,
						currentState,
						null
					));
				}
			}

			// add the beginning state
			State<T> beginningState = states.get(BEGINNING_STATE_NAME);;
			addTransitions(beginningState, -1, patterns, states);
			return new NFAFactoryImpl<T>(inputTypeSerializer, windowTime, new HashSet<>(states.values()), timeoutHandling);
		}
	}

	private static <T> void addTransitions(State<T> currentState, int patternPos, ArrayList<Pattern<T, ?>> patterns, Map<String, State<T>> states) {
		Pattern<T, ?> succeedingPattern = patterns.get(patternPos + 1);
		State<T> succeedingState = states.get(succeedingPattern.getName());

		if (shouldRepeatPattern(patternPos, patterns)) {
			expandRepeatingPattern(currentState, patternPos, patterns, states);
		} else {
			currentState.addStateTransition(new StateTransition<T>(
				StateTransitionAction.TAKE,
				succeedingState,
				(FilterFunction<T>) succeedingPattern.getFilterFunction()
			));

			if (shouldAddSelfTransition(succeedingPattern))  {
				addTransitionToSelf(succeedingPattern, succeedingState);
			}
			if (isPatternOptional(succeedingPattern)) {
				addOptionalTransitions(currentState, patternPos, patterns, states);
			}
		}
	}

	private static <T> void addOptionalTransitions(State<T> currentState, int patternPos, ArrayList<Pattern<T, ?>> patterns, Map<String, State<T>> states) {
		int firstNonOptionalPattern = findFirstNonOptionalPattern(patterns, patternPos + 1);

		for (int optionalPatternPos = patternPos + 2;
				optionalPatternPos < Math.min(firstNonOptionalPattern + 1, patterns.size());
				optionalPatternPos++) {

			Pattern<T, ?> optionalPattern = patterns.get(optionalPatternPos);
			State<T> optionalState = states.get(optionalPattern.getName());
			currentState.addStateTransition(new StateTransition<>(
				StateTransitionAction.TAKE,
				optionalState,
				(FilterFunction<T>) optionalPattern.getFilterFunction()));
		}
	}

	/**
	 * Expand a pattern number of times and connect expanded states. E.g. count(3) wil result in:
	 *
	 * +-----+  +-------+  +-------+
	 * |State+->|State#1+->|State#2+
	 * +--+--+  +-------+  +--+----+
	 */
	private static <T> void expandRepeatingPattern(State<T> currentState, int patternPos,
													ArrayList<Pattern<T, ?>> patterns, Map<String, State<T>> states) {
		Pattern<T, ?> succeedingPattern = patterns.get(patternPos + 1);
		State<T> succeedingState = states.get(succeedingPattern.getName());
		Pattern<T, ?> currentPattern = patterns.get(patternPos);

		State<T> currentRepeatingState = null;
		State<T> nextRepeatingState = currentState;
		for (int i = 1; i < currentPattern.getMaxCount(); i++) {
			currentRepeatingState = nextRepeatingState;
			nextRepeatingState = new State<>(
				currentState.getName() + "#" + i,
				State.StateType.Normal);
			states.put(nextRepeatingState.getName(), nextRepeatingState);
			currentRepeatingState.addStateTransition(new StateTransition<T>(
				StateTransitionAction.TAKE,
				nextRepeatingState,
				(FilterFunction<T>) currentPattern.getFilterFunction()));

			// Add a transition around optional pattern.
			// count(2,3) will result in:
			// +-----+  +-------+   +-------+  +----+
			// |State+->|State#1+-->|State#2+->|Next|
			// +--+--+  +-------+   +--+----+  +-+--+
			//              |                    ^
			//              +--------------------+
			if (i >= currentPattern.getMinCount()) {
				currentRepeatingState.addStateTransition(new StateTransition<T>(
					StateTransitionAction.TAKE,
					succeedingState,
					(FilterFunction<T>) succeedingPattern.getFilterFunction()));
			}
		}
		nextRepeatingState.addStateTransition(new StateTransition<T>(
			StateTransitionAction.TAKE,
			succeedingState,
			(FilterFunction<T>) succeedingPattern.getFilterFunction()));
	}

	private static <T> boolean shouldRepeatPattern(int patternPos, ArrayList<Pattern<T, ?>> patterns) {
		if (patternPos == -1) {
			return false;
		}

		Pattern<T, ?> pattern = patterns.get(patternPos);
		return pattern.getMinCount() != 1 || pattern.getMaxCount() != 1;
	}

	private static <T> void addTransitionToSelf(Pattern<T, ?> succeedingPattern, State<T> succeedingState) {
		succeedingState.addStateTransition(new StateTransition<T>(
			StateTransitionAction.TAKE,
			succeedingState,
			(FilterFunction<T>) succeedingPattern.getFilterFunction()));
	}

	private static <T> boolean shouldAddSelfTransition(Pattern<T, ?> succeedingPattern) {
		return succeedingPattern.getQuantifier() == Quantifier.ZERO_OR_MANY
			|| succeedingPattern.getQuantifier() == Quantifier.ONE_OR_MANY;
	}

	private static <T> int findFirstNonOptionalPattern(ArrayList<Pattern<T, ?>> patterns, int startPos) {
		int pos = startPos;
		for (; pos < patterns.size(); pos++) {
			Pattern<T, ?> pattern = patterns.get(pos);
			if (!isPatternOptional(pattern)) {
				return pos;
			}
		}

		return pos;
	}

	private static <T> Map<String, State<T>> createStatesFrom(ArrayList<Pattern<T, ?>> patterns) {
		Map<String, State<T>> states = new HashMap<>();

		boolean foundNonOptionalPattern = false;
		for (int i = patterns.size() - 1; i >= 0; i--) {
			Pattern<T, ?> pattern = patterns.get(i);
			State.StateType stateType = foundNonOptionalPattern ? State.StateType.Normal
																: State.StateType.Final;
			State<T> newState = new State<>(pattern.getName(), stateType);
			foundNonOptionalPattern |= !isPatternOptional(pattern);
			states.put(newState.getName(), newState);
		}

		State<T> beginningState = new State<>(BEGINNING_STATE_NAME, State.StateType.Start);
		states.put(BEGINNING_STATE_NAME, beginningState);
		return states;
	}

	private static <T> boolean isPatternOptional(Pattern<T, ?> pattern) {
		return pattern.getQuantifier() == Quantifier.ZERO_OR_MANY
			|| pattern.getQuantifier() == Quantifier.OPTIONAL
			|| pattern.getMinCount() == 0;
	}

	private static <T> ArrayList<Pattern<T, ?>> createPatternsList(Pattern<T, ?> pattern) {
		ArrayList<Pattern<T, ?>> patterns = new ArrayList<>();
		Pattern<T, ?> currentPattern = pattern;
		while (currentPattern != null) {
			patterns.add(currentPattern);
			currentPattern = currentPattern.getPrevious();
		}

		Collections.reverse(patterns);
		return patterns;
	}

	/**
	 * Factory interface for {@link NFA}.
	 *
	 * @param <T> Type of the input events which are processed by the NFA
	 */
	public interface NFAFactory<T> extends Serializable {
		NFA<T> createNFA();
	}

	/**
	 * Implementation of the {@link NFAFactory} interface.
	 * <p>
	 * The implementation takes the input type serializer, the window time and the set of
	 * states and their transitions to be able to create an NFA from them.
	 *
	 * @param <T> Type of the input events which are processed by the NFA
	 */
	private static class NFAFactoryImpl<T> implements NFAFactory<T> {

		private static final long serialVersionUID = 8939783698296714379L;

		private final TypeSerializer<T> inputTypeSerializer;
		private final long windowTime;
		private final Collection<State<T>> states;
		private final boolean timeoutHandling;

		private NFAFactoryImpl(
			TypeSerializer<T> inputTypeSerializer,
			long windowTime,
			Collection<State<T>> states,
			boolean timeoutHandling) {

			this.inputTypeSerializer = inputTypeSerializer;
			this.windowTime = windowTime;
			this.states = states;
			this.timeoutHandling = timeoutHandling;
		}

		@Override
		public NFA<T> createNFA() {
			NFA<T> result =  new NFA<>(inputTypeSerializer.duplicate(), windowTime, timeoutHandling);

			result.addStates(states);

			return result;
		}
	}
}
