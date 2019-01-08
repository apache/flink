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
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.State;
import org.apache.flink.cep.nfa.StateTransition;
import org.apache.flink.cep.nfa.StateTransitionAction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.GroupPattern;
import org.apache.flink.cep.pattern.MalformedPatternException;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier;
import org.apache.flink.cep.pattern.Quantifier.Times;
import org.apache.flink.cep.pattern.conditions.AndCondition;
import org.apache.flink.cep.pattern.conditions.BooleanConditions;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.NotCondition;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Compiler class containing methods to compile a {@link Pattern} into a {@link NFA} or a
 * {@link NFAFactory}.
 */
public class NFACompiler {

	protected static final String ENDING_STATE_NAME = "$endState$";

	/**
	 * Compiles the given pattern into a {@link NFAFactory}. The NFA factory can be used to create
	 * multiple NFAs.
	 *
	 * @param pattern Definition of sequence pattern
	 * @param timeoutHandling True if the NFA shall return timed out event patterns
	 * @param <T> Type of the input events
	 * @return Factory for NFAs corresponding to the given pattern
	 */
	@SuppressWarnings("unchecked")
	public static <T> NFAFactory<T> compileFactory(
		final Pattern<T, ?> pattern,
		boolean timeoutHandling) {
		if (pattern == null) {
			// return a factory for empty NFAs
			return new NFAFactoryImpl<>(0, Collections.<State<T>>emptyList(), timeoutHandling);
		} else {
			final NFAFactoryCompiler<T> nfaFactoryCompiler = new NFAFactoryCompiler<>(pattern);
			nfaFactoryCompiler.compileFactory();
			return new NFAFactoryImpl<>(nfaFactoryCompiler.getWindowTime(), nfaFactoryCompiler.getStates(), timeoutHandling);
		}
	}

	/**
	 * Converts a {@link Pattern} into graph of {@link State}. It enables sharing of
	 * compilation state across methods.
	 *
	 * @param <T>
	 */
	static class NFAFactoryCompiler<T> {

		private final NFAStateNameHandler stateNameHandler = new NFAStateNameHandler();
		private final Map<String, State<T>> stopStates = new HashMap<>();
		private final List<State<T>> states = new ArrayList<>();

		private long windowTime = 0;
		private GroupPattern<T, ?> currentGroupPattern;
		private Map<GroupPattern<T, ?>, Boolean> firstOfLoopMap = new HashMap<>();
		private Pattern<T, ?> currentPattern;
		private Pattern<T, ?> followingPattern;
		private final AfterMatchSkipStrategy afterMatchSkipStrategy;
		private Map<String, State<T>> originalStateMap = new HashMap<>();

		NFAFactoryCompiler(final Pattern<T, ?> pattern) {
			this.currentPattern = pattern;
			afterMatchSkipStrategy = pattern.getAfterMatchSkipStrategy();
		}

		/**
		 * Compiles the given pattern into a {@link NFAFactory}. The NFA factory can be used to create
		 * multiple NFAs.
		 */
		void compileFactory() {
			if (currentPattern.getQuantifier().getConsumingStrategy() == Quantifier.ConsumingStrategy.NOT_FOLLOW) {
				throw new MalformedPatternException("NotFollowedBy is not supported as a last part of a Pattern!");
			}

			checkPatternNameUniqueness();

			checkPatternSkipStrategy();

			// we're traversing the pattern from the end to the beginning --> the first state is the final state
			State<T> sinkState = createEndingState();
			// add all the normal states
			sinkState = createMiddleStates(sinkState);
			// add the beginning state
			createStartState(sinkState);
		}

		AfterMatchSkipStrategy getAfterMatchSkipStrategy(){
			return afterMatchSkipStrategy;
		}

		List<State<T>> getStates() {
			return states;
		}

		long getWindowTime() {
			return windowTime;
		}

		/**
		 * Check pattern after match skip strategy.
		 */
		private void checkPatternSkipStrategy() {
			if (afterMatchSkipStrategy.getPatternName().isPresent()) {
				String patternName = afterMatchSkipStrategy.getPatternName().get();
				Pattern<T, ?> pattern = currentPattern;
				while (pattern.getPrevious() != null && !pattern.getName().equals(patternName)) {
					pattern = pattern.getPrevious();
				}

				// pattern name match check.
				if (!pattern.getName().equals(patternName)) {
					throw new MalformedPatternException("The pattern name specified in AfterMatchSkipStrategy " +
						"can not be found in the given Pattern");
				}
			}
		}

		/**
		 * Check if there are duplicate pattern names. If yes, it
		 * throws a {@link MalformedPatternException}.
		 */
		private void checkPatternNameUniqueness() {
			// make sure there is no pattern with name "$endState$"
			stateNameHandler.checkNameUniqueness(ENDING_STATE_NAME);
			Pattern patternToCheck = currentPattern;
			while (patternToCheck != null) {
				checkPatternNameUniqueness(patternToCheck);
				patternToCheck = patternToCheck.getPrevious();
			}
			stateNameHandler.clear();
		}

		/**
		 * Check if the given pattern's name is already used or not. If yes, it
		 * throws a {@link MalformedPatternException}.
		 *
		 * @param pattern The pattern to be checked
		 */
		private void checkPatternNameUniqueness(final Pattern pattern) {
			if (pattern instanceof GroupPattern) {
				Pattern patternToCheck = ((GroupPattern) pattern).getRawPattern();
				while (patternToCheck != null) {
					checkPatternNameUniqueness(patternToCheck);
					patternToCheck = patternToCheck.getPrevious();
				}
			} else {
				stateNameHandler.checkNameUniqueness(pattern.getName());
			}
		}

		/**
		 * Retrieves list of conditions resulting in Stop state and names of the corresponding NOT patterns.
		 *
		 * <p>A current not condition can be produced in two cases:
		 * <ol>
		 *     <li>the previous pattern is a {@link Quantifier.ConsumingStrategy#NOT_FOLLOW}</li>
		 *     <li>exists a backward path of {@link Quantifier.QuantifierProperty#OPTIONAL} patterns to
		 *       {@link Quantifier.ConsumingStrategy#NOT_FOLLOW}</li>
		 * </ol>
		 *
		 * <p><b>WARNING:</b> for more info on the second case see: {@link NFAFactoryCompiler#copyWithoutTransitiveNots(State)}
		 *
		 * @return list of not conditions with corresponding names
		 */
		private List<Tuple2<IterativeCondition<T>, String>> getCurrentNotCondition() {
			List<Tuple2<IterativeCondition<T>, String>> notConditions = new ArrayList<>();

			Pattern<T, ? extends T> previousPattern = currentPattern;
			while (previousPattern.getPrevious() != null && (
				previousPattern.getPrevious().getQuantifier().hasProperty(Quantifier.QuantifierProperty.OPTIONAL) ||
				previousPattern.getPrevious().getQuantifier().getConsumingStrategy() == Quantifier.ConsumingStrategy.NOT_FOLLOW)) {

				previousPattern = previousPattern.getPrevious();

				if (previousPattern.getQuantifier().getConsumingStrategy() == Quantifier.ConsumingStrategy.NOT_FOLLOW) {
					final IterativeCondition<T> notCondition = getTakeCondition(previousPattern);
					notConditions.add(Tuple2.of(notCondition, previousPattern.getName()));
				}
			}
			return notConditions;
		}

		/**
		 * Creates the dummy Final {@link State} of the NFA graph.
		 * @return dummy Final state
		 */
		private State<T> createEndingState() {
			State<T> endState = createState(ENDING_STATE_NAME, State.StateType.Final);
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

				if (currentPattern.getQuantifier().getConsumingStrategy() == Quantifier.ConsumingStrategy.NOT_FOLLOW) {
					//skip notFollow patterns, they are converted into edge conditions
				} else if (currentPattern.getQuantifier().getConsumingStrategy() == Quantifier.ConsumingStrategy.NOT_NEXT) {
					final State<T> notNext = createState(currentPattern.getName(), State.StateType.Normal);
					final IterativeCondition<T> notCondition = getTakeCondition(currentPattern);
					final State<T> stopState = createStopState(notCondition, currentPattern.getName());

					if (lastSink.isFinal()) {
						//so that the proceed to final is not fired
						notNext.addIgnore(lastSink, new NotCondition<>(notCondition));
					} else {
						notNext.addProceed(lastSink, new NotCondition<>(notCondition));
					}
					notNext.addProceed(stopState, notCondition);
					lastSink = notNext;
				} else {
					lastSink = convertPattern(lastSink);
				}

				// we traverse the pattern graph backwards
				followingPattern = currentPattern;
				currentPattern = currentPattern.getPrevious();

				final Time currentWindowTime = currentPattern.getWindowTime();
				if (currentWindowTime != null && currentWindowTime.toMilliseconds() < windowTime) {
					// the window time is the global minimum of all window times of each state
					windowTime = currentWindowTime.toMilliseconds();
				}
			}
			return lastSink;
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

		private State<T> convertPattern(final State<T> sinkState) {
			final State<T> lastSink;

			final Quantifier quantifier = currentPattern.getQuantifier();
			if (quantifier.hasProperty(Quantifier.QuantifierProperty.LOOPING)) {

				// if loop has started then all notPatterns previous to the optional states are no longer valid
				setCurrentGroupPatternFirstOfLoop(false);
				final State<T> sink = copyWithoutTransitiveNots(sinkState);
				final State<T> looping = createLooping(sink);

				setCurrentGroupPatternFirstOfLoop(true);
				lastSink = createTimesState(looping, sinkState, currentPattern.getTimes());
			} else if (quantifier.hasProperty(Quantifier.QuantifierProperty.TIMES)) {
				lastSink = createTimesState(sinkState, sinkState, currentPattern.getTimes());
			} else {
				lastSink = createSingletonState(sinkState);
			}
			addStopStates(lastSink);

			return lastSink;
		}

		/**
		 * Creates a state with {@link State.StateType#Normal} and adds it to the collection of created states.
		 * Should be used instead of instantiating with new operator.
		 *
		 * @return the created state
		 */
		private State<T> createState(String name, State.StateType stateType) {
			String stateName = stateNameHandler.getUniqueInternalName(name);
			State<T> state = new State<>(stateName, stateType);
			states.add(state);
			return state;
		}

		private State<T> createStopState(final IterativeCondition<T> notCondition, final String name) {
			// We should not duplicate the notStates. All states from which we can stop should point to the same one.
			State<T> stopState = stopStates.get(name);
			if (stopState == null) {
				stopState = createState(name, State.StateType.Stop);
				stopState.addTake(notCondition);
				stopStates.put(name, stopState);
			}
			return stopState;
		}

		/**
		 * This method creates an alternative state that is target for TAKE transition from an optional State.
		 * Accepting an event in optional State discards all not Patterns that were present before it.
		 *
		 * <p>E.g for a Pattern begin("a").notFollowedBy("b").followedByAny("c").optional().followedByAny("d")
		 * a sequence like : {a c b d} is a valid match, but {a b d} is not.
		 *
		 * <p><b>NOTICE:</b> This method creates copy only if it necessary.
		 *
		 * @param sinkState a state to create copy without transitive nots
		 * @return the copy of the state itself if no modifications were needed
		 */
		private State<T> copyWithoutTransitiveNots(final State<T> sinkState) {
			final List<Tuple2<IterativeCondition<T>, String>> currentNotCondition = getCurrentNotCondition();

			if (currentNotCondition.isEmpty() ||
				!currentPattern.getQuantifier().hasProperty(Quantifier.QuantifierProperty.OPTIONAL)) {
				//we do not create an alternative path if we are NOT in an OPTIONAL state or there is no NOTs prior to
				//the optional state
				return sinkState;
			}

			final State<T> copyOfSink = createState(sinkState.getName(), sinkState.getStateType());

			for (StateTransition<T> tStateTransition : sinkState.getStateTransitions()) {

				if (tStateTransition.getAction() == StateTransitionAction.PROCEED) {
					State<T> targetState = tStateTransition.getTargetState();
					boolean remove = false;
					if (targetState.isStop()) {
						for (Tuple2<IterativeCondition<T>, String> notCondition : currentNotCondition) {
							if (targetState.getName().equals(notCondition.f1)) {
								remove = true;
							}
						}
					} else {
						targetState = copyWithoutTransitiveNots(tStateTransition.getTargetState());
					}

					if (!remove) {
						copyOfSink.addStateTransition(tStateTransition.getAction(), targetState, tStateTransition.getCondition());
					}
				} else {
					copyOfSink.addStateTransition(
							tStateTransition.getAction(),
							tStateTransition.getTargetState().equals(tStateTransition.getSourceState())
									? copyOfSink
									: tStateTransition.getTargetState(),
							tStateTransition.getCondition()
					);
				}

			}
			return copyOfSink;
		}

		private State<T> copy(final State<T> state) {
			final State<T> copyOfState = createState(
				NFAStateNameHandler.getOriginalNameFromInternal(state.getName()),
				state.getStateType());
			for (StateTransition<T> tStateTransition : state.getStateTransitions()) {
				copyOfState.addStateTransition(
					tStateTransition.getAction(),
					tStateTransition.getTargetState().equals(tStateTransition.getSourceState())
							? copyOfState
							: tStateTransition.getTargetState(),
					tStateTransition.getCondition());
			}
			return copyOfState;
		}

		private void addStopStates(final State<T> state) {
			for (Tuple2<IterativeCondition<T>, String> notCondition: getCurrentNotCondition()) {
				final State<T> stopState = createStopState(notCondition.f0, notCondition.f1);
				state.addProceed(stopState, notCondition.f0);
			}
		}

		private void addStopStateToLooping(final State<T> loopingState) {
			if (followingPattern != null &&
					followingPattern.getQuantifier().getConsumingStrategy() == Quantifier.ConsumingStrategy.NOT_FOLLOW) {
				final IterativeCondition<T> notCondition = getTakeCondition(followingPattern);
				final State<T> stopState = createStopState(notCondition, followingPattern.getName());
				loopingState.addProceed(stopState, notCondition);
			}
		}

		/**
		 * Creates a "complex" state consisting of given number of states with
		 * same {@link IterativeCondition}.
		 *
		 * @param sinkState the state that the created state should point to
		 * @param proceedState state that the state being converted should proceed to
		 * @param times     number of times the state should be copied
		 * @return the first state of the "complex" state, next state should point to it
		 */
		@SuppressWarnings("unchecked")
		private State<T> createTimesState(final State<T> sinkState, final State<T> proceedState, Times times) {
			State<T> lastSink = sinkState;
			setCurrentGroupPatternFirstOfLoop(false);
			final IterativeCondition<T> untilCondition = (IterativeCondition<T>) currentPattern.getUntilCondition();
			final IterativeCondition<T> innerIgnoreCondition = extendWithUntilCondition(
				getInnerIgnoreCondition(currentPattern),
				untilCondition,
				false);
			final IterativeCondition<T> takeCondition = extendWithUntilCondition(
				getTakeCondition(currentPattern),
				untilCondition,
				true);

			if (currentPattern.getQuantifier().hasProperty(Quantifier.QuantifierProperty.GREEDY) &&
				times.getFrom() != times.getTo()) {
				if (untilCondition != null) {
					State<T> sinkStateCopy = copy(sinkState);
					originalStateMap.put(sinkState.getName(), sinkStateCopy);
				}
				updateWithGreedyCondition(sinkState, takeCondition);
			}

			for (int i = times.getFrom(); i < times.getTo(); i++) {
				lastSink = createSingletonState(lastSink, proceedState, takeCondition, innerIgnoreCondition, true);
				addStopStateToLooping(lastSink);
			}
			for (int i = 0; i < times.getFrom() - 1; i++) {
				lastSink = createSingletonState(lastSink, null, takeCondition, innerIgnoreCondition, false);
				addStopStateToLooping(lastSink);
			}
			// we created the intermediate states in the loop, now we create the start of the loop.
			setCurrentGroupPatternFirstOfLoop(true);
			return createSingletonState(
				lastSink,
				proceedState,
				takeCondition,
				getIgnoreCondition(currentPattern),
				currentPattern.getQuantifier().hasProperty(Quantifier.QuantifierProperty.OPTIONAL));
		}

		/**
		 * Marks the current group pattern as the head of the TIMES quantifier or not.
		 *
		 * @param isFirstOfLoop whether the current group pattern is the head of the TIMES quantifier
		 */
		@SuppressWarnings("unchecked")
		private void setCurrentGroupPatternFirstOfLoop(boolean isFirstOfLoop) {
			if (currentPattern instanceof GroupPattern) {
				firstOfLoopMap.put((GroupPattern<T, ?>) currentPattern, isFirstOfLoop);
			}
		}

		/**
		 * Checks if the current group pattern is the head of the TIMES/LOOPING quantifier or not a
		 * TIMES/LOOPING quantifier pattern.
		 */
		private boolean isCurrentGroupPatternFirstOfLoop() {
			if (firstOfLoopMap.containsKey(currentGroupPattern)) {
				return firstOfLoopMap.get(currentGroupPattern);
			} else {
				return true;
			}
		}

		/**
		 * Checks if the given pattern is the head pattern of the current group pattern.
		 *
		 * @param pattern the pattern to be checked
		 * @return {@code true} iff the given pattern is in a group pattern and it is the head pattern of the
		 * group pattern, {@code false} otherwise
		 */
		private boolean headOfGroup(Pattern<T, ?> pattern) {
			return currentGroupPattern != null && pattern.getPrevious() == null;
		}

		/**
		 * Checks if the given pattern is optional. If the given pattern is the head of a group pattern,
		 * the optional status depends on the group pattern.
		 */
		private boolean isPatternOptional(Pattern<T, ?> pattern) {
			if (headOfGroup(pattern)) {
				return isCurrentGroupPatternFirstOfLoop() &&
					currentGroupPattern.getQuantifier().hasProperty(Quantifier.QuantifierProperty.OPTIONAL);
			} else {
				return pattern.getQuantifier().hasProperty(Quantifier.QuantifierProperty.OPTIONAL);
			}
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
				sinkState,
				getTakeCondition(currentPattern),
				getIgnoreCondition(currentPattern),
				isPatternOptional(currentPattern));
		}

		/**
		 * Creates a simple single state. For an OPTIONAL state it also consists
		 * of a similar state without the PROCEED edge, so that for each PROCEED transition branches
		 * in computation state graph  can be created only once.
		 *
		 * @param ignoreCondition condition that should be applied to IGNORE transition
		 * @param sinkState state that the state being converted should point to
		 * @param proceedState state that the state being converted should proceed to
		 * @param isOptional whether the state being converted is optional
		 * @return the created state
		 */
		@SuppressWarnings("unchecked")
		private State<T> createSingletonState(final State<T> sinkState,
			final State<T> proceedState,
			final IterativeCondition<T> takeCondition,
			final IterativeCondition<T> ignoreCondition,
			final boolean isOptional) {
			if (currentPattern instanceof GroupPattern) {
				return createGroupPatternState((GroupPattern) currentPattern, sinkState, proceedState, isOptional);
			}

			final State<T> singletonState = createState(currentPattern.getName(), State.StateType.Normal);
			// if event is accepted then all notPatterns previous to the optional states are no longer valid
			final State<T> sink = copyWithoutTransitiveNots(sinkState);
			singletonState.addTake(sink, takeCondition);

			// if no element accepted the previous nots are still valid.
			final IterativeCondition<T> proceedCondition = getTrueFunction();

			// for the first state of a group pattern, its PROCEED edge should point to the following state of
			// that group pattern and the edge will be added at the end of creating the NFA for that group pattern
			if (isOptional && !headOfGroup(currentPattern)) {
				if (currentPattern.getQuantifier().hasProperty(Quantifier.QuantifierProperty.GREEDY)) {
					final IterativeCondition<T> untilCondition =
						(IterativeCondition<T>) currentPattern.getUntilCondition();
					if (untilCondition != null) {
						singletonState.addProceed(
							originalStateMap.get(proceedState.getName()),
							new AndCondition<>(proceedCondition, untilCondition));
					}
					singletonState.addProceed(proceedState,
						untilCondition != null
							? new AndCondition<>(proceedCondition, new NotCondition<>(untilCondition))
							: proceedCondition);
				} else {
					singletonState.addProceed(proceedState, proceedCondition);
				}
			}

			if (ignoreCondition != null) {
				final State<T> ignoreState;
				if (isOptional) {
					ignoreState = createState(currentPattern.getName(), State.StateType.Normal);
					ignoreState.addTake(sink, takeCondition);
					ignoreState.addIgnore(ignoreCondition);
					addStopStates(ignoreState);
				} else {
					ignoreState = singletonState;
				}
				singletonState.addIgnore(ignoreState, ignoreCondition);
			}
			return singletonState;
		}

		/**
		 * Create all the states for the group pattern.
		 *
		 * @param groupPattern the group pattern to create the states for
		 * @param sinkState the state that the group pattern being converted should point to
		 * @param proceedState the state that the group pattern being converted should proceed to
		 * @param isOptional whether the group pattern being converted is optional
		 * @return the first state of the states of the group pattern
		 */
		private State<T> createGroupPatternState(
			final GroupPattern<T, ?> groupPattern,
			final State<T> sinkState,
			final State<T> proceedState,
			final boolean isOptional) {
			final IterativeCondition<T> proceedCondition = getTrueFunction();

			Pattern<T, ?> oldCurrentPattern = currentPattern;
			Pattern<T, ?> oldFollowingPattern = followingPattern;
			GroupPattern<T, ?> oldGroupPattern = currentGroupPattern;

			State<T> lastSink = sinkState;
			currentGroupPattern = groupPattern;
			currentPattern = groupPattern.getRawPattern();
			lastSink = createMiddleStates(lastSink);
			lastSink = convertPattern(lastSink);
			if (isOptional) {
				// for the first state of a group pattern, its PROCEED edge should point to
				// the following state of that group pattern
				lastSink.addProceed(proceedState, proceedCondition);
			}
			currentPattern = oldCurrentPattern;
			followingPattern = oldFollowingPattern;
			currentGroupPattern = oldGroupPattern;
			return lastSink;
		}

		/**
		 * Create the states for the group pattern as a looping one.
		 *
		 * @param groupPattern the group pattern to create the states for
		 * @param sinkState the state that the group pattern being converted should point to
		 * @return the first state of the states of the group pattern
		 */
		private State<T> createLoopingGroupPatternState(
			final GroupPattern<T, ?> groupPattern,
			final State<T> sinkState) {
			final IterativeCondition<T> proceedCondition = getTrueFunction();

			Pattern<T, ?> oldCurrentPattern = currentPattern;
			Pattern<T, ?> oldFollowingPattern = followingPattern;
			GroupPattern<T, ?> oldGroupPattern = currentGroupPattern;

			final State<T> dummyState = createState(currentPattern.getName(), State.StateType.Normal);
			State<T> lastSink = dummyState;
			currentGroupPattern = groupPattern;
			currentPattern = groupPattern.getRawPattern();
			lastSink = createMiddleStates(lastSink);
			lastSink = convertPattern(lastSink);
			lastSink.addProceed(sinkState, proceedCondition);
			dummyState.addProceed(lastSink, proceedCondition);
			currentPattern = oldCurrentPattern;
			followingPattern = oldFollowingPattern;
			currentGroupPattern = oldGroupPattern;
			return lastSink;
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
			if (currentPattern instanceof GroupPattern) {
				return createLoopingGroupPatternState((GroupPattern) currentPattern, sinkState);
			}
			final IterativeCondition<T> untilCondition = (IterativeCondition<T>) currentPattern.getUntilCondition();

			final IterativeCondition<T> ignoreCondition = extendWithUntilCondition(
				getInnerIgnoreCondition(currentPattern),
				untilCondition,
				false);
			final IterativeCondition<T> takeCondition = extendWithUntilCondition(
				getTakeCondition(currentPattern),
				untilCondition,
				true);

			IterativeCondition<T> proceedCondition = getTrueFunction();
			final State<T> loopingState = createState(currentPattern.getName(), State.StateType.Normal);

			if (currentPattern.getQuantifier().hasProperty(Quantifier.QuantifierProperty.GREEDY)) {
				if (untilCondition != null) {
					State<T> sinkStateCopy = copy(sinkState);
					loopingState.addProceed(sinkStateCopy, new AndCondition<>(proceedCondition, untilCondition));
					originalStateMap.put(sinkState.getName(), sinkStateCopy);
				}
				loopingState.addProceed(sinkState,
					untilCondition != null
						? new AndCondition<>(proceedCondition, new NotCondition<>(untilCondition))
						: proceedCondition);
				updateWithGreedyCondition(sinkState, getTakeCondition(currentPattern));
			} else {
				loopingState.addProceed(sinkState, proceedCondition);
			}
			loopingState.addTake(takeCondition);

			addStopStateToLooping(loopingState);

			if (ignoreCondition != null) {
				final State<T> ignoreState = createState(currentPattern.getName(), State.StateType.Normal);
				ignoreState.addTake(loopingState, takeCondition);
				ignoreState.addIgnore(ignoreCondition);
				loopingState.addIgnore(ignoreState, ignoreCondition);

				addStopStateToLooping(ignoreState);
			}
			return loopingState;
		}

		/**
		 * This method extends the given condition with stop(until) condition if necessary.
		 * The until condition needs to be applied only if both of the given conditions are not null.
		 *
		 * @param condition the condition to extend
		 * @param untilCondition the until condition to join with the given condition
		 * @param isTakeCondition whether the {@code condition} is for {@code TAKE} edge
		 * @return condition with AND applied or the original condition
		 */
		private IterativeCondition<T> extendWithUntilCondition(
				IterativeCondition<T> condition,
				IterativeCondition<T> untilCondition,
				boolean isTakeCondition) {
			if (untilCondition != null && condition != null) {
				return new AndCondition<>(new NotCondition<>(untilCondition), condition);
			} else if (untilCondition != null && isTakeCondition) {
				return new NotCondition<>(untilCondition);
			}

			return condition;
		}

		/**
		 * @return The {@link IterativeCondition condition} for the {@code IGNORE} edge
		 * that corresponds to the specified {@link Pattern} and extended with stop(until) condition
		 * if necessary. It is applicable only for inner states of a complex state like looping or times.
		 */
		@SuppressWarnings("unchecked")
		private IterativeCondition<T> getInnerIgnoreCondition(Pattern<T, ?> pattern) {
			Quantifier.ConsumingStrategy consumingStrategy = pattern.getQuantifier().getInnerConsumingStrategy();
			if (headOfGroup(pattern)) {
				// for the head pattern of a group pattern, we should consider the
				// inner consume strategy of the group pattern
				consumingStrategy = currentGroupPattern.getQuantifier().getInnerConsumingStrategy();
			}

			IterativeCondition<T> innerIgnoreCondition = null;
			switch (consumingStrategy) {
				case STRICT:
					innerIgnoreCondition = null;
					break;
				case SKIP_TILL_NEXT:
					innerIgnoreCondition = new NotCondition<>((IterativeCondition<T>) pattern.getCondition());
					break;
				case SKIP_TILL_ANY:
					innerIgnoreCondition = BooleanConditions.trueFunction();
					break;
			}

			if (currentGroupPattern != null && currentGroupPattern.getUntilCondition() != null) {
				innerIgnoreCondition = extendWithUntilCondition(
					innerIgnoreCondition,
					(IterativeCondition<T>) currentGroupPattern.getUntilCondition(),
					false);
			}
			return innerIgnoreCondition;
		}

		/**
		 * @return The {@link IterativeCondition condition} for the {@code IGNORE} edge
		 * that corresponds to the specified {@link Pattern} and extended with
		 * stop(until) condition if necessary. For more on strategy see {@link Quantifier}
		 */
		@SuppressWarnings("unchecked")
		private IterativeCondition<T> getIgnoreCondition(Pattern<T, ?> pattern) {
			Quantifier.ConsumingStrategy consumingStrategy = pattern.getQuantifier().getConsumingStrategy();
			if (headOfGroup(pattern)) {
				// for the head pattern of a group pattern, we should consider the inner consume strategy
				// of the group pattern if the group pattern is not the head of the TIMES/LOOPING quantifier;
				// otherwise, we should consider the consume strategy of the group pattern
				if (isCurrentGroupPatternFirstOfLoop()) {
					consumingStrategy = currentGroupPattern.getQuantifier().getConsumingStrategy();
				} else {
					consumingStrategy = currentGroupPattern.getQuantifier().getInnerConsumingStrategy();
				}
			}

			IterativeCondition<T> ignoreCondition = null;
			switch (consumingStrategy) {
				case STRICT:
					ignoreCondition = null;
					break;
				case SKIP_TILL_NEXT:
					ignoreCondition = new NotCondition<>((IterativeCondition<T>) pattern.getCondition());
					break;
				case SKIP_TILL_ANY:
					ignoreCondition = BooleanConditions.trueFunction();
					break;
			}

			if (currentGroupPattern != null && currentGroupPattern.getUntilCondition() != null) {
				ignoreCondition = extendWithUntilCondition(
					ignoreCondition,
					(IterativeCondition<T>) currentGroupPattern.getUntilCondition(),
					false);
			}
			return ignoreCondition;
		}

		/**
		 * @return the {@link IterativeCondition condition} for the {@code TAKE} edge
		 * that corresponds to the specified {@link Pattern} and extended with
		 * stop(until) condition if necessary.
		 */
		@SuppressWarnings("unchecked")
		private IterativeCondition<T> getTakeCondition(Pattern<T, ?> pattern) {
			IterativeCondition<T> takeCondition = (IterativeCondition<T>) pattern.getCondition();
			if (currentGroupPattern != null && currentGroupPattern.getUntilCondition() != null) {
				takeCondition = extendWithUntilCondition(
					takeCondition,
					(IterativeCondition<T>) currentGroupPattern.getUntilCondition(),
					true);
			}
			return takeCondition;
		}

		/**
		 * @return An true function extended with stop(until) condition if necessary.
		 */
		@SuppressWarnings("unchecked")
		private IterativeCondition<T> getTrueFunction() {
			IterativeCondition<T> trueCondition = BooleanConditions.trueFunction();
			if (currentGroupPattern != null && currentGroupPattern.getUntilCondition() != null) {
				trueCondition = extendWithUntilCondition(
					trueCondition,
					(IterativeCondition<T>) currentGroupPattern.getUntilCondition(),
					true);
			}
			return trueCondition;
		}

		private void updateWithGreedyCondition(
			State<T> state,
			IterativeCondition<T> takeCondition) {
			for (StateTransition<T> stateTransition : state.getStateTransitions()) {
				stateTransition.setCondition(
					new AndCondition<>(stateTransition.getCondition(), new NotCondition<>(takeCondition)));
			}
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
	 *
	 * <p>The implementation takes the input type serializer, the window time and the set of
	 * states and their transitions to be able to create an NFA from them.
	 *
	 * @param <T> Type of the input events which are processed by the NFA
	 */
	private static class NFAFactoryImpl<T> implements NFAFactory<T> {

		private static final long serialVersionUID = 8939783698296714379L;

		private final long windowTime;
		private final Collection<State<T>> states;
		private final boolean timeoutHandling;

		private NFAFactoryImpl(
				long windowTime,
				Collection<State<T>> states,
				boolean timeoutHandling) {

			this.windowTime = windowTime;
			this.states = states;
			this.timeoutHandling = timeoutHandling;
		}

		@Override
		public NFA<T> createNFA() {
			return new NFA<>(states, windowTime, timeoutHandling);
		}
	}
}
