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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.State;
import org.apache.flink.cep.nfa.StateTransition;
import org.apache.flink.cep.nfa.StateTransitionAction;
import org.apache.flink.cep.pattern.EventPattern;
import org.apache.flink.cep.pattern.Pattern;

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

	final static String BEGINNING_STATE_NAME = "$beginningState$";

	/**
	 * Compiles the given pattern into a {@link NFA}.
	 *
	 * @param pattern             Definition of sequence pattern
	 * @param inputTypeSerializer Serializer for the input type
	 * @param timeoutHandling     True if the NFA shall return timed out event patterns
	 * @param <T>                 Type of the input events
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
	 * @param pattern             Definition of sequence pattern
	 * @param inputTypeSerializer Serializer for the input type
	 * @param timeoutHandling     True if the NFA shall return timed out event patterns
	 * @param <T>                 Type of the input events
	 * @return Factory for NFAs corresponding to the given pattern
	 */
	@SuppressWarnings("unchecked")
	public static <T> NFAFactory<T> compileFactory(
		Pattern<T, ?> pattern,
		TypeSerializer<T> inputTypeSerializer,
		boolean timeoutHandling) {
		if (pattern == null) {

			// return a factory for empty NFAs
			return new NFAFactoryImpl<>(inputTypeSerializer, 0,
										Collections.<State<T>>emptyList(), timeoutHandling);

		} else {

			long windowTime =
				pattern.getWindowTime() != null ? pattern.getWindowTime().toMilliseconds() : 0L;

			// add the beginning state
			final State<T> beginningState;
			Map<String, State<T>> states = new HashMap<>();
			Collection<Tuple2<State<T>, Pattern<T, ?>>> startStates =
				compileStates(pattern, states);

			if (states.containsKey(BEGINNING_STATE_NAME)) {
				beginningState = states.get(BEGINNING_STATE_NAME);
			} else {
				beginningState = new State<>(BEGINNING_STATE_NAME, State.StateType.Start);
				states.put(BEGINNING_STATE_NAME, beginningState);
			}

			for (Tuple2<State<T>, Pattern<T, ?>> stateAndPattern : startStates) {
				State<T> state = stateAndPattern.f0;
				Pattern<T, ?> statePattern = stateAndPattern.f1;
				beginningState.addStateTransition(new StateTransition<>(
					StateTransitionAction.TAKE, state,
					statePattern instanceof EventPattern
					? ((EventPattern) statePattern).getFilterFunction()
					: null));
			}

			return new NFAFactoryImpl<>(inputTypeSerializer, windowTime,
										new HashSet<>(states.values()), timeoutHandling);
		}
	}

	@SuppressWarnings("unchecked")
	private static <T> Collection<Tuple2<State<T>, Pattern<T, ?>>> compileStates(
		Pattern<T, ?> pattern, Map<String, State<T>> states
	) {
		Collection<Tuple2<State<T>, Pattern<T, ?>>> startStates = new ArrayList<>();

		if (pattern instanceof EventPattern) {
			EventPattern<T, ?> eventPattern = (EventPattern<T, ?>) pattern;
			State<T> currentState = new State<>(eventPattern.getName(), State.StateType.Final);
			states.put(eventPattern.getName(), currentState);
			startStates.addAll(eventPattern.setStates(states, currentState, null));
		} else {
			for (Pattern<T, ? extends T> parent : pattern.getParents()) {
				startStates.addAll(compileStates(parent, states));
			}
		}

		return startStates;
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
			NFA<T> result = new NFA<>(inputTypeSerializer.duplicate(), windowTime, timeoutHandling);
			result.addStates(states);
			return result;
		}
	}
}
