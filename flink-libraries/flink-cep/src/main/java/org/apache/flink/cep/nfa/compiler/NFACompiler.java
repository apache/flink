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
			State<T> beginningState;

			if (states.containsKey(BEGINNING_STATE_NAME)) {
				beginningState = states.get(BEGINNING_STATE_NAME);
			} else {
				beginningState = new State<>(BEGINNING_STATE_NAME, State.StateType.Start);
				states.put(BEGINNING_STATE_NAME, beginningState);
			}

			addTransitions(beginningState, -1, patterns, states);

			return new NFAFactoryImpl<T>(inputTypeSerializer, windowTime, new HashSet<>(states.values()), timeoutHandling);
		}
	}

	private static <T> void addTransitions(State<T> currentState, int patternPos, ArrayList<Pattern<T, ?>> patterns, Map<String, State<T>> states) {
		Pattern<T, ?> succeedingPattern = patterns.get(patternPos + 1);
		State<T> succeedingState = states.get(succeedingPattern.getName());

		if (patternPos != -1 ) {
			Pattern<T, ?> currentPattern = patterns.get(patternPos);
			if (currentPattern.getMaxCount() != 1 && currentPattern.getMinCount() != 1) {
				State<T> cS = null;
				State<T> nS = currentState;
				for (int i = 1; i < currentPattern.getMaxCount(); i++) {
					cS = nS;
					nS = new State<>(
						currentState.getName() + "#" + i,
						State.StateType.Normal);
					states.put(nS.getName(), nS);
					cS.addStateTransition(new StateTransition<T>(
						StateTransitionAction.TAKE,
						nS,
						(FilterFunction<T>) currentPattern.getFilterFunction()));
				}
				nS.addStateTransition(new StateTransition<T>(
					StateTransitionAction.TAKE,
					succeedingState,
					(FilterFunction<T>) succeedingPattern.getFilterFunction()));
				return;
			}
		}

		currentState.addStateTransition(new StateTransition<T>(
			StateTransitionAction.TAKE,
			succeedingState,
			(FilterFunction<T>) succeedingPattern.getFilterFunction()
		));

		if (succeedingPattern.getQuantifier() == Quantifier.ONE_OR_MORE
			|| succeedingPattern.getQuantifier() == Quantifier.ZERO_OR_MORE)  {
			succeedingState.addStateTransition(new StateTransition<T>(
				StateTransitionAction.TAKE,
				succeedingState,
				(FilterFunction<T>) succeedingPattern.getFilterFunction()));
		}
		if (succeedingPattern.getQuantifier() == Quantifier.OPTIONAL
			|| succeedingPattern.getQuantifier() == Quantifier.ZERO_OR_MORE) {
			int firstNonOptionalPattern = findFirstNonOptionalPattern(patterns, patternPos + 1);
			if (firstNonOptionalPattern == patterns.size()) {
				currentState = new State<>(
					currentState.getName(),
					State.StateType.Final,
					currentState.getStateTransitions());
				states.put(currentState.getName(), currentState);
			}

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

	}

	private static <T> int findFirstNonOptionalPattern(ArrayList<Pattern<T, ?>> patterns, int startPos) {
		int pos = startPos;
		for (; pos < patterns.size(); pos++) {
			Pattern<T, ?> pattern = patterns.get(pos);
			if (pattern.getQuantifier() != Quantifier.OPTIONAL
				&& pattern.getQuantifier() != Quantifier.ZERO_OR_MORE) {
				return pos;
			}
		}

		return pos;
	}

	private static <T> Map<String, State<T>> createStatesFrom(ArrayList<Pattern<T, ?>> patterns) {
		Map<String, State<T>> states = new HashMap<>();

		for (int i = 0; i < patterns.size(); i++) {
			Pattern<T, ?> pattern = patterns.get(i);
			State<T> newState;
			if (i != patterns.size() - 1) {
				newState = new State<>(pattern.getName(), State.StateType.Normal);
			} else {
				newState = new State<>(pattern.getName(), State.StateType.Final);
			}
			states.put(newState.getName(), newState);
		}
		return states;
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
