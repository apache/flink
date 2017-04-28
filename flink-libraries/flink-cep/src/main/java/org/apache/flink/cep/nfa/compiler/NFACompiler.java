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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.State;
import org.apache.flink.cep.nfa.StateTransition;
import org.apache.flink.cep.nfa.StateTransitionAction;
import org.apache.flink.cep.pattern.MalformedPatternException;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier;
import org.apache.flink.cep.pattern.conditions.BooleanConditions;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.NotCondition;
import org.apache.flink.streaming.api.windowing.time.Time;

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
		 *
		 * @param sinkState the state that last state should point to (always the Final state)
		 * @return the next state after Start in the resulting graph
		 */
		private State<T> createMiddleStates(final State<T> sinkState) {

			State<T> lastSink = sinkState;
			while (currentPattern.getPrevious() != null) {
				lastSink = convertPattern(lastSink);
				currentPattern = currentPattern.getPrevious();

				final Time currentWindowTime = currentPattern.getWindowTime();
				if (currentWindowTime != null && currentWindowTime.toMilliseconds() < windowTime) {
					// the window time is the global minimum of all window times of each state
					windowTime = currentWindowTime.toMilliseconds();
				}
			}

			return lastSink;
		}

		private State<T> convertPattern(final State<T> sinkState) {
			final State<T> lastSink;
			checkPatternNameUniqueness();
			usedNames.add(currentPattern.getName());

			final Quantifier quantifier = currentPattern.getQuantifier();
			if (quantifier.hasProperty(Quantifier.QuantifierProperty.LOOPING)) {
				final State<T> looping = createLooping(sinkState);

				if (!quantifier.hasProperty(Quantifier.QuantifierProperty.OPTIONAL)) {
					lastSink = createFirstMandatoryStateOfLoop(looping);
				} else {
					lastSink = createWaitingStateForZeroOrMore(looping, sinkState);
				}
			} else if (quantifier.hasProperty(Quantifier.QuantifierProperty.TIMES)) {
				lastSink = createTimesState(sinkState, currentPattern.getTimes());
			} else {
				lastSink = createSingletonState(sinkState);
			}

			return lastSink;
		}

		/**
		 * Creates a pair of states that enables relaxed strictness before a zeroOrMore looping state.
		 *
		 * @param loopingState the first state of zeroOrMore complex state
		 * @param lastSink     the state that the looping one points to
		 * @return the newly created state
		 */
		@SuppressWarnings("unchecked")
		private State<T> createWaitingStateForZeroOrMore(final State<T> loopingState, final State<T> lastSink) {
			final IterativeCondition<T> currentFunction = (IterativeCondition<T>)currentPattern.getCondition();

			final State<T> followByState = createNormalState();
			followByState.addProceed(lastSink, BooleanConditions.<T>trueFunction());
			followByState.addTake(loopingState, currentFunction);

			final IterativeCondition<T> ignoreFunction = getIgnoreCondition(currentPattern);
			if (ignoreFunction != null) {
				final State<T> followByStateWithoutProceed = createNormalState();
				followByState.addIgnore(followByStateWithoutProceed, ignoreFunction);
				followByStateWithoutProceed.addIgnore(ignoreFunction);
				followByStateWithoutProceed.addTake(loopingState, currentFunction);
			}

			return followByState;
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
		 *
		 * @param sinkState the state that Start state should point to (always first state of middle states)
		 * @return created state
		 */
		@SuppressWarnings("unchecked")
		private State<T> createStartState(State<T> sinkState) {
			final State<T> beginningState = convertPattern(sinkState);
			beginningState.makeStart();

			return beginningState;
		}

		/**
		 * Creates a "complex" state consisting of given number of states with
		 * same {@link IterativeCondition}
		 *
		 * @param sinkState the state that the created state should point to
		 * @param times     number of times the state should be copied
		 * @return the first state of the "complex" state, next state should point to it
		 */
		private State<T> createTimesState(final State<T> sinkState, int times) {
			State<T> lastSink = sinkState;
			for (int i = 0; i < times - 1; i++) {
				lastSink = createSingletonState(lastSink, getInnerIgnoreCondition(currentPattern), false);
			}

			final IterativeCondition<T> currentFilterFunction = (IterativeCondition<T>) currentPattern.getCondition();
			final IterativeCondition<T> ignoreCondition = getIgnoreCondition(currentPattern);

			// we created the intermediate states in the loop, now we create the start of the loop.
			if (!currentPattern.getQuantifier().hasProperty(Quantifier.QuantifierProperty.OPTIONAL)) {
				return createSingletonState(lastSink, ignoreCondition, false);
			}

			final State<T> singletonState = createNormalState();
			singletonState.addTake(lastSink, currentFilterFunction);
			singletonState.addProceed(sinkState, BooleanConditions.<T>trueFunction());

			if (ignoreCondition != null) {
				State<T> ignoreState = createNormalState();
				ignoreState.addTake(lastSink, currentFilterFunction);
				ignoreState.addIgnore(ignoreCondition);
				singletonState.addIgnore(ignoreState, ignoreCondition);
			}
			return singletonState;
		}

		/**
		 * Creates a simple single state. For an OPTIONAL state it also consists
		 * of a similar state without the PROCEED edge, so that for each PROCEED transition branches
		 * in computation state graph  can be created only once.
		 *
		 * @param sinkState state that the state being converted should point to
		 * @return the created state
		 */
		@SuppressWarnings("unchecked")
		private State<T> createSingletonState(final State<T> sinkState) {
			return createSingletonState(
				sinkState,
				getIgnoreCondition(currentPattern),
				currentPattern.getQuantifier().hasProperty(Quantifier.QuantifierProperty.OPTIONAL));
		}

		/**
		 * Creates a simple single state. For an OPTIONAL state it also consists
		 * of a similar state without the PROCEED edge, so that for each PROCEED transition branches
		 * in computation state graph  can be created only once.
		 *
		 * @param ignoreCondition condition that should be applied to IGNORE transition
		 * @param sinkState state that the state being converted should point to
		 * @return the created state
		 */
		@SuppressWarnings("unchecked")
		private State<T> createSingletonState(final State<T> sinkState, final IterativeCondition<T> ignoreCondition, final boolean isOptional) {
			final IterativeCondition<T> currentFilterFunction = (IterativeCondition<T>) currentPattern.getCondition();
			final IterativeCondition<T> trueFunction = BooleanConditions.trueFunction();

			final State<T> singletonState = createNormalState();
			singletonState.addTake(sinkState, currentFilterFunction);

			if (isOptional) {
				singletonState.addProceed(sinkState, trueFunction);
			}

			if (ignoreCondition != null) {
				final State<T> ignoreState;
				if (isOptional) {
					ignoreState = createNormalState();
					ignoreState.addTake(sinkState, currentFilterFunction);
				} else {
					ignoreState = singletonState;
				}
				singletonState.addIgnore(ignoreState, ignoreCondition);
			}
			return singletonState;
		}

		/**
		 * Patterns with quantifiers AT_LEAST_ONE_* are created as a pair of states: a singleton state and
		 * looping state. This method creates the first of the two.
		 *
		 * @param sinkState the state the newly created state should point to, it should be a looping state
		 * @return the newly created state
		 */
		@SuppressWarnings("unchecked")
		private State<T> createFirstMandatoryStateOfLoop(final State<T> sinkState) {

			final IterativeCondition<T> currentFilterFunction = (IterativeCondition<T>) currentPattern.getCondition();
			final State<T> firstState = createNormalState();

			firstState.addTake(sinkState, currentFilterFunction);
			final IterativeCondition<T> ignoreCondition = getIgnoreCondition(currentPattern);
			if (ignoreCondition != null) {
				firstState.addIgnore(ignoreCondition);
			}
			return firstState;
		}

		/**
		 * Creates the given state as a looping one. Looping state is one with TAKE edge to itself and
		 * PROCEED edge to the sinkState. It also consists of a similar state without the PROCEED edge, so that
		 * for each PROCEED transition branches in computation state graph  can be created only once.
		 *
		 * @param sinkState the state that the converted state should point to
		 * @return the first state of the created complex state
		 */
		@SuppressWarnings("unchecked")
		private State<T> createLooping(final State<T> sinkState) {
			final IterativeCondition<T> filterFunction = (IterativeCondition<T>) currentPattern.getCondition();
			final IterativeCondition<T> ignoreCondition = getInnerIgnoreCondition(currentPattern);
			final IterativeCondition<T> trueFunction = BooleanConditions.trueFunction();

			final State<T> loopingState = createNormalState();
			loopingState.addProceed(sinkState, trueFunction);
			loopingState.addTake(filterFunction);

			if (ignoreCondition != null) {
				final State<T> ignoreState = createNormalState();
				ignoreState.addTake(loopingState, filterFunction);
				ignoreState.addIgnore(ignoreCondition);
				loopingState.addIgnore(ignoreState, ignoreCondition);
			}

			return loopingState;
		}

		/**
		 * Creates a state with {@link State.StateType#Normal} and adds it to the collection of created states.
		 * Should be used instead of instantiating with new operator.
		 *
		 * @return the created state
		 */
		private State<T> createNormalState() {
			final State<T> state = new State<>(currentPattern.getName(), State.StateType.Normal);
			states.add(state);
			return state;
		}

		/**
		 * @return The {@link IterativeCondition condition} for the {@code IGNORE} edge
		 * that corresponds to the specified {@link Pattern}. It is applicable only for inner states of a complex
		 * state like looping or times.
		 */
		@SuppressWarnings("unchecked")
		private IterativeCondition<T> getInnerIgnoreCondition(Pattern<T, ?> pattern) {
			switch (pattern.getQuantifier().getInnerConsumingStrategy()) {
				case STRICT:
					return null;
				case SKIP_TILL_NEXT:
					return new NotCondition<>((IterativeCondition<T>) pattern.getCondition());
				case SKIP_TILL_ANY:
					return BooleanConditions.trueFunction();
			}
			return null;
		}

		/**
		 * @return The {@link IterativeCondition condition} for the {@code IGNORE} edge
		 * that corresponds to the specified {@link Pattern}. For more on strategy see {@link Quantifier}
		 */
		@SuppressWarnings("unchecked")
		private IterativeCondition<T> getIgnoreCondition(Pattern<T, ?> pattern) {
			switch (pattern.getQuantifier().getConsumingStrategy()) {
				case STRICT:
					return null;
				case SKIP_TILL_NEXT:
					return new NotCondition<>((IterativeCondition<T>) pattern.getCondition());
				case SKIP_TILL_ANY:
					return BooleanConditions.trueFunction();
			}
			return null;
		}
	}

	/**
	 * Used for migrating CEP graphs prior to 1.3. It removes the dummy start, adds the dummy end, and translates all
	 * states to consuming ones by moving all TAKEs and IGNOREs to the next state. This method assumes each state
	 * has at most one TAKE and one IGNORE and name of each state is unique. No PROCEED transition is allowed!
	 *
	 * @param oldStartState dummy start state of old graph
	 * @param <T>           type of events
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
