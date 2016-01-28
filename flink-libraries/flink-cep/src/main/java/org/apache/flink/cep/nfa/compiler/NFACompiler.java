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
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
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
	 * @param <T> Type of the input events
	 * @return Non-deterministic finite automaton representing the given pattern
	 */
	public static <T> NFA<T> compile(Pattern<T, ?> pattern, TypeSerializer<T> inputTypeSerializer) {
		NFAFactory<T> factory = compileFactory(pattern, inputTypeSerializer);

		return factory.createNFA();
	}

	/**
	 * Compiles the given pattern into a {@link NFAFactory}. The NFA factory can be used to create
	 * multiple NFAs.
	 *
	 * @param pattern Definition of sequence pattern
	 * @param inputTypeSerializer Serializer for the input type
	 * @param <T> Type of the input events
	 * @return Factory for NFAs corresponding to the given pattern
	 */
	@SuppressWarnings("unchecked")
	public static <T> NFAFactory<T> compileFactory(Pattern<T, ?> pattern, TypeSerializer<T> inputTypeSerializer) {
		if (pattern == null) {
			// return a factory for empty NFAs
			return new NFAFactoryImpl<T>(inputTypeSerializer, 0, Collections.<State<T>>emptyList());
		} else {
			// set of all generated states
			Map<String, State<T>> states = new HashMap<>();
			long windowTime;

			Pattern<T, ?> succeedingPattern;
			State<T> succeedingState;
			Pattern<T, ?> currentPattern = pattern;

			// we're traversing the pattern from the end to the beginning --> the first state is the final state
			State<T> currentState = new State<>(currentPattern.getName(), State.StateType.Final);

			states.put(currentPattern.getName(), currentState);

			windowTime = currentPattern.getWindowTime() != null ? currentPattern.getWindowTime().toMilliseconds() : 0L;

			while (currentPattern.getPrevious() != null) {
				succeedingPattern = currentPattern;
				succeedingState = currentState;
				currentPattern = currentPattern.getPrevious();

				Time currentWindowTime = currentPattern.getWindowTime();

				if (currentWindowTime != null && currentWindowTime.toMilliseconds() < windowTime) {
					// the window time is the global minimum of all window times of each state
					windowTime = currentWindowTime.toMilliseconds();
				}

				if (states.containsKey(currentPattern.getName())) {
					currentState = states.get(currentPattern.getName());
				} else {
					currentState = new State<>(currentPattern.getName(), State.StateType.Normal);
					states.put(currentState.getName(), currentState);
				}

				currentState.addStateTransition(new StateTransition<T>(
					StateTransitionAction.TAKE,
					succeedingState,
					(FilterFunction<T>) succeedingPattern.getFilterFunction()));

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
			final State<T> beginningState;

			if (states.containsKey(BEGINNING_STATE_NAME)) {
				beginningState = states.get(BEGINNING_STATE_NAME);
			} else {
				beginningState = new State<>(BEGINNING_STATE_NAME, State.StateType.Start);
				states.put(BEGINNING_STATE_NAME, beginningState);
			}

			beginningState.addStateTransition(new StateTransition<T>(
				StateTransitionAction.TAKE,
				currentState,
				(FilterFunction<T>) currentPattern.getFilterFunction()
			));

			NFA<T> nfa = new NFA<T>(inputTypeSerializer, windowTime);
			nfa.addStates(states.values());

			return new NFAFactoryImpl<T>(inputTypeSerializer, windowTime, new HashSet<>(states.values()));
		}
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

		private NFAFactoryImpl(TypeSerializer<T> inputTypeSerializer, long windowTime, Collection<State<T>> states) {
			this.inputTypeSerializer = inputTypeSerializer;
			this.windowTime = windowTime;
			this.states = states;
		}

		@Override
		public NFA<T> createNFA() {
			NFA<T> result =  new NFA<>(inputTypeSerializer.duplicate(), windowTime);

			result.addStates(states);

			return result;
		}
	}
}
