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
import org.apache.flink.cep.nfa.Action;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.State;
import org.apache.flink.cep.nfa.StateTransition;
import org.apache.flink.cep.pattern.FollowedByPattern;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class NFACompiler {

	protected final static String BEGINNING_STATE_NAME = "$beginningState$";

	public static <T> NFA<T> compile(Pattern<T, ?> pattern, TypeSerializer<T> inputTypeSerializer) {
		NFAFactory<T> factory = compileFactory(pattern, inputTypeSerializer);

		return factory.createNFA();
	}

	public static <T> NFAFactory<T> compileFactory(Pattern<T, ?> pattern, TypeSerializer<T> inputTypeSerializer) {
		if (pattern == null) {
			return new NFAFactoryImpl<T>(inputTypeSerializer, 0, Collections.<State<T>>emptyList());
		} else {
			Map<String, State<T>> states = new HashMap<>();
			long windowTime;

			Pattern<T, ?> succeedingPattern = null;
			State<T> succeedingState = null;
			Pattern<T, ?> currentPattern = pattern;

			State<T> currentState = new State<>(currentPattern.getName(), State.StateType.Final);

			states.put(currentPattern.getName(), currentState);

			windowTime = currentPattern.getWindowTime() != null ? currentPattern.getWindowTime().toMilliseconds() : 0L;

			while (currentPattern.getPrevious() != null) {
				succeedingPattern = currentPattern;
				succeedingState = currentState;
				currentPattern = currentPattern.getPrevious();

				Time currentWindowTime = currentPattern.getWindowTime();

				if (currentWindowTime != null && currentWindowTime.toMilliseconds() < windowTime) {
					windowTime = currentWindowTime.toMilliseconds();
				}

				if (states.containsKey(currentPattern.getName())) {
					currentState = states.get(currentPattern.getName());
				} else {
					currentState = new State<>(currentPattern.getName(), State.StateType.Normal);
					states.put(currentState.getName(), currentState);
				}

				if (succeedingPattern instanceof FollowedByPattern) {
					currentState.addStateTransition(new StateTransition<T>(
						Action.TAKE,
						succeedingState,
						(FilterFunction<T>) succeedingPattern.getFilterFunction()));
					currentState.addStateTransition(new StateTransition<T>(
						Action.IGNORE,
						currentState,
						null
					));
				} else {
					currentState.addStateTransition(new StateTransition<T>(
						Action.TAKE,
						succeedingState,
						(FilterFunction<T>) succeedingPattern.getFilterFunction()));
				}
			}

			// beginning state
			final State<T> beginningState;

			if (states.containsKey(BEGINNING_STATE_NAME)) {
				beginningState = states.get(BEGINNING_STATE_NAME);
			} else {
				beginningState = new State<>(BEGINNING_STATE_NAME, State.StateType.Start);
				states.put(BEGINNING_STATE_NAME, beginningState);
			}

			beginningState.addStateTransition(new StateTransition<T>(
				Action.TAKE,
				currentState,
				(FilterFunction<T>) currentPattern.getFilterFunction()
			));

			NFA<T> nfa = new NFA(inputTypeSerializer, windowTime);
			nfa.addStates(states.values());

			return new NFAFactoryImpl(inputTypeSerializer, windowTime, states.values());
		}
	}

	public interface NFAFactory<T> extends Serializable {
		NFA<T> createNFA();
	}

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
