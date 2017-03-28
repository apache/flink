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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.State;
import org.apache.flink.cep.nfa.StateTransition;
import org.apache.flink.cep.nfa.StateTransitionAction;
import org.apache.flink.cep.pattern.FilterFunctions;
import org.apache.flink.cep.pattern.FollowedByPattern;
import org.apache.flink.cep.pattern.MalformedPatternException;
import org.apache.flink.cep.pattern.NotFilterFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier;
import org.apache.flink.cep.pattern.Quantifier.QuantifierProperty;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Compiler class containing methods to compile a {@link Pattern} into a {@link NFA} or a
 * {@link NFAFactory}.
 */
public class NFACompiler {

	protected static final String ENDING_STATE_NAME = "$endState$";

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
		final Pattern<T, ?> pattern,
		final TypeSerializer<T> inputTypeSerializer,
		boolean timeoutHandling) {
		if (pattern == null) {
			// return a factory for empty NFAs
			return new NFAFactoryImpl<>(inputTypeSerializer, 0, Collections.<State<T>>emptyList(), timeoutHandling);
		} else {
			final NFAFactoryCompiler<T> nfaFactoryCompiler = new NFAFactoryCompiler<>(pattern);
			nfaFactoryCompiler.compileFactory();
			return new NFAFactoryImpl<>(inputTypeSerializer, nfaFactoryCompiler.getWindowTime(), nfaFactoryCompiler.getStates(), timeoutHandling);
		}
	}

	/**
	 * Converts a {@link Pattern} into graph of {@link State}. It enables sharing of
	 * compilation state across methods.
	 *
	 * @param <T>
	 */
	private static class NFAFactoryCompiler<T> {

		private final Set<String> usedNames = new HashSet<>();
		private final List<State<T>> states = new ArrayList<>();

		private long windowTime = 0;
		private Pattern<T, ?> currentPattern;

		NFAFactoryCompiler(final Pattern<T, ?> pattern) {
			this.currentPattern = pattern;
		}

		/**
		 * Compiles the given pattern into a {@link NFAFactory}. The NFA factory can be used to create
		 * multiple NFAs.
		 */
		void compileFactory() {
			// we're traversing the pattern from the end to the beginning --> the first state is the final state
			State<T> sinkState = createEndingState();
			// add all the normal states
			sinkState = createMiddleStates(sinkState);
			// add the beginning state
			createStartState(sinkState);
		}

		List<State<T>> getStates() {
			return states;
		}

		long getWindowTime() {
			return windowTime;
		}

		/**
		 * Creates the dummy Final {@link State} of the NFA graph.
		 * @return dummy Final state
		 */
		private State<T> createEndingState() {
			State<T> endState = new State<>(ENDING_STATE_NAME, State.StateType.Final);
			states.add(endState);
			usedNames.add(ENDING_STATE_NAME);

			windowTime = currentPattern.getWindowTime() != null ? currentPattern.getWindowTime().toMilliseconds() : 0L;
			return endState;
		}

		/**
		 * Creates all the states between Start and Final state.
		 * @param sinkState the state that last state should point to (always the Final state)
		 * @return the next state after Start in the resulting graph
		 */
		private State<T> createMiddleStates(final State<T> sinkState) {

			State<T> lastSink = sinkState;
			while (currentPattern.getPrevious() != null) {
				checkPatternNameUniqueness();

				State<T> sourceState = new State<>(currentPattern.getName(), State.StateType.Normal);
				states.add(sourceState);
				usedNames.add(sourceState.getName());

				if (currentPattern.getQuantifier().hasProperty(QuantifierProperty.LOOPING)) {
					convertToLooping(sourceState, lastSink);

					if (currentPattern.getQuantifier().hasProperty(QuantifierProperty.AT_LEAST_ONE)) {
						sourceState = createFirstMandatoryStateOfLoop(sourceState, State.StateType.Normal);
						states.add(sourceState);
						usedNames.add(sourceState.getName());
					}
				} else if (currentPattern.getQuantifier() == Quantifier.TIMES) {
					sourceState = convertToTimesState(sourceState, lastSink, currentPattern.getTimes());
				} else {
					convertToSingletonState(sourceState, lastSink);
				}

				currentPattern = currentPattern.getPrevious();
				lastSink = sourceState;

				final Time currentWindowTime = currentPattern.getWindowTime();
				if (currentWindowTime != null && currentWindowTime.toMilliseconds() < windowTime) {
					// the window time is the global minimum of all window times of each state
					windowTime = currentWindowTime.toMilliseconds();
				}
			}

			return lastSink;
		}

		private void checkPatternNameUniqueness() {
			if (usedNames.contains(currentPattern.getName())) {
				throw new MalformedPatternException(
					"Duplicate pattern name: " + currentPattern.getName() + ". " +
					"Pattern names must be unique.");
			}
		}

		/**
		 * Creates the Start {@link State} of the resulting NFA graph.
		 * @param sinkState the state that Start state should point to (alwyas first state of middle states)
		 * @return created state
		 */
		@SuppressWarnings("unchecked")
		private State<T> createStartState(State<T> sinkState) {
			checkPatternNameUniqueness();

			final State<T> beginningState;
			if (currentPattern.getQuantifier().hasProperty(QuantifierProperty.LOOPING)) {
				final State<T> loopingState;
				if (currentPattern.getQuantifier().hasProperty(QuantifierProperty.AT_LEAST_ONE)) {
					loopingState = new State<>(currentPattern.getName(), State.StateType.Normal);
					beginningState = createFirstMandatoryStateOfLoop(loopingState, State.StateType.Start);
					states.add(loopingState);
				} else {
					loopingState = new State<>(currentPattern.getName(), State.StateType.Start);
					beginningState = loopingState;
				}
				convertToLooping(loopingState, sinkState, true);
			} else  {
				if (currentPattern.getQuantifier() == Quantifier.TIMES && currentPattern.getTimes() > 1) {
					final State<T> timesState = new State<>(currentPattern.getName(), State.StateType.Normal);
					states.add(timesState);
					sinkState = convertToTimesState(timesState, sinkState, currentPattern.getTimes() - 1);
				}

				beginningState = new State<>(currentPattern.getName(), State.StateType.Start);
				convertToSingletonState(beginningState, sinkState);
			}

			states.add(beginningState);
			usedNames.add(beginningState.getName());

			return beginningState;
		}

		/**
		 * Converts the given state into a "complex" state consisting of given number of states with
		 * same {@link FilterFunction}
		 *
		 * @param sourceState the state to be converted
		 * @param sinkState the state that the converted state should point to
		 * @param times number of times the state should be copied
		 * @return the first state of the "complex" state, next state should point to it
		 */
		private State<T> convertToTimesState(final State<T> sourceState, final State<T> sinkState, int times) {
			convertToSingletonState(sourceState, sinkState);
			State<T> lastSink;
			State<T> firstState = sourceState;
			for (int i = 0; i < times - 1; i++) {
				lastSink = firstState;
				firstState = new State<>(currentPattern.getName(), State.StateType.Normal);
				states.add(firstState);
				convertToSingletonState(firstState, lastSink);
			}
			return firstState;
		}

		/**
		 * Converts the given state into a simple single state. For an OPTIONAL state it also consists
		 * of a similar state without the PROCEED edge, so that for each PROCEED transition branches
		 * in computation state graph  can be created only once.
		 *
		 * @param sourceState the state to be converted
		 * @param sinkState state that the state being converted should point to
		 */
		@SuppressWarnings("unchecked")
		private void convertToSingletonState(final State<T> sourceState, final State<T> sinkState) {

			final FilterFunction<T> currentFilterFunction = (FilterFunction<T>) currentPattern.getFilterFunction();
			final FilterFunction<T> trueFunction = FilterFunctions.trueFunction();
			sourceState.addTake(sinkState, currentFilterFunction);

			if (currentPattern.getQuantifier() == Quantifier.OPTIONAL) {
				sourceState.addProceed(sinkState, trueFunction);
			}

			if (currentPattern instanceof FollowedByPattern) {
				final State<T> ignoreState;
				if (currentPattern.getQuantifier() == Quantifier.OPTIONAL) {
					ignoreState = new State<>(currentPattern.getName(), State.StateType.Normal);
					ignoreState.addTake(sinkState, currentFilterFunction);
					states.add(ignoreState);
				} else {
					ignoreState = sourceState;
				}
				sourceState.addIgnore(ignoreState, trueFunction);
			}
		}

		/**
		 * Patterns with quantifiers AT_LEAST_ONE_* are converted into pair of states: a singleton state and
		 * looping state. This method creates the first of the two.
		 *
		 * @param sinkState the state the newly created state should point to, it should be a looping state
		 * @param stateType the type of the created state, as the NFA graph can also start wit AT_LEAST_ONE_*
		 * @return the newly created state
		 */
		@SuppressWarnings("unchecked")
		private State<T> createFirstMandatoryStateOfLoop(final State<T> sinkState, final State.StateType stateType) {

			final FilterFunction<T> currentFilterFunction = (FilterFunction<T>) currentPattern.getFilterFunction();
			final State<T> firstState = new State<>(currentPattern.getName(), stateType);

			firstState.addTake(sinkState, currentFilterFunction);
			if (currentPattern instanceof FollowedByPattern) {
				if (currentPattern.getQuantifier().hasProperty(QuantifierProperty.EAGER)) {
					firstState.addIgnore(new NotFilterFunction<>(currentFilterFunction));
				} else {
					firstState.addIgnore(FilterFunctions.<T>trueFunction());
				}
			}
			return firstState;
		}

		/**
		 * Converts the given state into looping one. Looping state is one with TAKE edge to itself and
		 * PROCEED edge to the sinkState. It also consists of a similar state without the PROCEED edge, so that
		 * for each PROCEED transition branches in computation state graph  can be created only once.
		 *
		 * <p>If this looping state is first of a graph we should treat the {@link Pattern} as {@link FollowedByPattern}
		 * to enable combinations.
		 *
		 * @param sourceState  the state to converted
		 * @param sinkState    the state that the converted state should point to
		 * @param isFirstState if the looping state is first of a graph
		 */
		@SuppressWarnings("unchecked")
		private void convertToLooping(final State<T> sourceState, final State<T> sinkState, boolean isFirstState) {

			final FilterFunction<T> filterFunction = (FilterFunction<T>) currentPattern.getFilterFunction();
			final FilterFunction<T> trueFunction = FilterFunctions.<T>trueFunction();

			sourceState.addProceed(sinkState, trueFunction);
			sourceState.addTake(filterFunction);
			if (currentPattern instanceof FollowedByPattern || isFirstState) {
				final State<T> ignoreState = new State<>(
					currentPattern.getName(),
					State.StateType.Normal);


				final FilterFunction<T> ignoreCondition;
				if (currentPattern.getQuantifier().hasProperty(QuantifierProperty.EAGER)) {
					ignoreCondition = new NotFilterFunction<>(filterFunction);
				} else {
					ignoreCondition = trueFunction;
				}

				sourceState.addIgnore(ignoreState, ignoreCondition);
				ignoreState.addTake(sourceState, filterFunction);
				ignoreState.addIgnore(ignoreState, ignoreCondition);
				states.add(ignoreState);
			}
		}

		/**
		 * Converts the given state into looping one. Looping state is one with TAKE edge to itself and
		 * PROCEED edge to the sinkState. It also consists of a similar state without the PROCEED edge, so that
		 * for each PROCEED transition branches in computation state graph  can be created only once.
		 *
		 * @param sourceState the state to converted
		 * @param sinkState   the state that the converted state should point to
		 */
		private void convertToLooping(final State<T> sourceState, final State<T> sinkState) {
			convertToLooping(sourceState, sinkState, false);
		}
	}

	/**
	 * Used for migrating CEP graphs prior to 1.3. It removes the dummy start, adds the dummy end, and translates all
	 * states to consuming ones by moving all TAKEs and IGNOREs to the next state. This method assumes each state
	 * has at most one TAKE and one IGNORE and name of each state is unique. No PROCEED transition is allowed!
	 *
	 * @param oldStartState dummy start state of old graph
	 * @param <T> type of events
	 * @return map of new states, where key is the name of a state and value is the state itself
	 */
	@Internal
	public static <T> Map<String, State<T>> migrateGraph(State<T> oldStartState) {
		State<T> oldFirst = oldStartState;
		State<T> oldSecond = oldStartState.getStateTransitions().iterator().next().getTargetState();

		StateTransition<T> oldFirstToSecondTake = Iterators.find(
			oldFirst.getStateTransitions().iterator(),
			new Predicate<StateTransition<T>>() {
				@Override
				public boolean apply(@Nullable StateTransition<T> input) {
					return input != null && input.getAction() == StateTransitionAction.TAKE;
				}

			});

		StateTransition<T> oldFirstIgnore = Iterators.find(
			oldFirst.getStateTransitions().iterator(),
			new Predicate<StateTransition<T>>() {
				@Override
				public boolean apply(@Nullable StateTransition<T> input) {
					return input != null && input.getAction() == StateTransitionAction.IGNORE;
				}

			}, null);

		StateTransition<T> oldSecondToThirdTake = Iterators.find(
			oldSecond.getStateTransitions().iterator(),
			new Predicate<StateTransition<T>>() {
				@Override
				public boolean apply(@Nullable StateTransition<T> input) {
					return input != null && input.getAction() == StateTransitionAction.TAKE;
				}

			}, null);

		final Map<String, State<T>> convertedStates = new HashMap<>();
		State<T> newSecond;
		State<T> newFirst = new State<>(oldSecond.getName(), State.StateType.Start);
		convertedStates.put(newFirst.getName(), newFirst);
		while (oldSecondToThirdTake != null) {

			newSecond = new State<T>(oldSecondToThirdTake.getTargetState().getName(), State.StateType.Normal);
			convertedStates.put(newSecond.getName(), newSecond);
			newFirst.addTake(newSecond, oldFirstToSecondTake.getCondition());

			if (oldFirstIgnore != null) {
				newFirst.addIgnore(oldFirstIgnore.getCondition());
			}

			oldFirst = oldSecond;

			oldFirstToSecondTake = Iterators.find(
				oldFirst.getStateTransitions().iterator(),
				new Predicate<StateTransition<T>>() {
					@Override
					public boolean apply(@Nullable StateTransition<T> input) {
						return input != null && input.getAction() == StateTransitionAction.TAKE;
					}

				});

			oldFirstIgnore = Iterators.find(
				oldFirst.getStateTransitions().iterator(),
				new Predicate<StateTransition<T>>() {
					@Override
					public boolean apply(@Nullable StateTransition<T> input) {
						return input != null && input.getAction() == StateTransitionAction.IGNORE;
					}

				}, null);

			oldSecond = oldSecondToThirdTake.getTargetState();

			oldSecondToThirdTake = Iterators.find(
				oldSecond.getStateTransitions().iterator(),
				new Predicate<StateTransition<T>>() {
					@Override
					public boolean apply(@Nullable StateTransition<T> input) {
						return input != null && input.getAction() == StateTransitionAction.TAKE;
					}

				}, null);

			newFirst = newSecond;
		}

		final State<T> endingState = new State<>(ENDING_STATE_NAME, State.StateType.Final);

		newFirst.addTake(endingState, oldFirstToSecondTake.getCondition());

		if (oldFirstIgnore != null) {
			newFirst.addIgnore(oldFirstIgnore.getCondition());
		}

		convertedStates.put(endingState.getName(), endingState);

		return convertedStates;
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
