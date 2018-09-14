/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.Preconditions;

import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewKeyedStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewOperatorStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.deepDummyCopy;

public class PrioritizedOperatorSubtaskStateTest extends TestLogger {

	private final Random random = new Random(0x42);

	/**
	 * This tests attempts to test (almost) the full space of significantly different options for verifying and
	 * prioritizing {@link OperatorSubtaskState} options for local recovery over primary/remote state handles.
	 */
	@Test
	public void testPrioritization() {

		for (int i = 0; i < 81; ++i) { // 3^4 possible configurations.

			OperatorSubtaskState primaryAndFallback = generateForConfiguration(i);

			for (int j = 0; j < 9; ++j) { // we test 3^2 configurations.
				// mode 0: one valid state handle (deep copy of original).
				// mode 1: empty StateHandleCollection.
				// mode 2: one invalid state handle (e.g. wrong key group, different meta data)
				int modeFirst = j % 3;
				OperatorSubtaskState bestAlternative = createAlternativeSubtaskState(primaryAndFallback, modeFirst);
				int modeSecond = (j / 3) % 3;
				OperatorSubtaskState secondBestAlternative = createAlternativeSubtaskState(primaryAndFallback, modeSecond);

				List<OperatorSubtaskState> orderedAlternativesList =
					Arrays.asList(bestAlternative, secondBestAlternative);
				List<OperatorSubtaskState> validAlternativesList = new ArrayList<>(3);
				if (modeFirst == 0) {
					validAlternativesList.add(bestAlternative);
				}
				if (modeSecond == 0) {
					validAlternativesList.add(secondBestAlternative);
				}
				validAlternativesList.add(primaryAndFallback);

				PrioritizedOperatorSubtaskState.Builder builder =
					new PrioritizedOperatorSubtaskState.Builder(primaryAndFallback, orderedAlternativesList);

				PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskState = builder.build();

				OperatorSubtaskState[] validAlternatives =
					validAlternativesList.toArray(new OperatorSubtaskState[validAlternativesList.size()]);

				OperatorSubtaskState[] onlyPrimary =
					new OperatorSubtaskState[]{primaryAndFallback};

				Assert.assertTrue(checkResultAsExpected(
					OperatorSubtaskState::getManagedOperatorState,
					PrioritizedOperatorSubtaskState::getPrioritizedManagedOperatorState,
					prioritizedOperatorSubtaskState,
					primaryAndFallback.getManagedOperatorState().size() == 1 ? validAlternatives : onlyPrimary));

				Assert.assertTrue(checkResultAsExpected(
					OperatorSubtaskState::getManagedKeyedState,
					PrioritizedOperatorSubtaskState::getPrioritizedManagedKeyedState,
					prioritizedOperatorSubtaskState,
					primaryAndFallback.getManagedKeyedState().size() == 1 ? validAlternatives : onlyPrimary));

				Assert.assertTrue(checkResultAsExpected(
					OperatorSubtaskState::getRawOperatorState,
					PrioritizedOperatorSubtaskState::getPrioritizedRawOperatorState,
					prioritizedOperatorSubtaskState,
					primaryAndFallback.getRawOperatorState().size() == 1 ? validAlternatives : onlyPrimary));

				Assert.assertTrue(checkResultAsExpected(
					OperatorSubtaskState::getRawKeyedState,
					PrioritizedOperatorSubtaskState::getPrioritizedRawKeyedState,
					prioritizedOperatorSubtaskState,
					primaryAndFallback.getRawKeyedState().size() == 1 ? validAlternatives : onlyPrimary));
			}
		}
	}

	/**
	 * Generator for all 3^4 = 81 possible configurations of a OperatorSubtaskState:
	 * - 4 different sub-states:
	 *      managed/raw + operator/keyed.
	 * - 3 different options per sub-state:
	 *      empty (simulate no state), single handle (simulate recovery), 2 handles (simulate e.g. rescaling)
	 */
	private OperatorSubtaskState generateForConfiguration(int conf) {

		Preconditions.checkState(conf >= 0 && conf <= 80); // 3^4
		final int numModes = 3;

		KeyGroupRange keyGroupRange = new KeyGroupRange(0, 4);
		KeyGroupRange keyGroupRange1 = new KeyGroupRange(0, 2);
		KeyGroupRange keyGroupRange2 = new KeyGroupRange(3, 4);

		int div = 1;
		int mode = (conf / div) % numModes;
		StateObjectCollection<OperatorStateHandle> s1 =
			mode == 0 ?
				StateObjectCollection.empty() :
				mode == 1 ?
					new StateObjectCollection<>(
						Collections.singletonList(createNewOperatorStateHandle(2, random))) :
					new StateObjectCollection<>(
						Arrays.asList(
							createNewOperatorStateHandle(2, random),
							createNewOperatorStateHandle(2, random)));
		div *= numModes;
		mode = (conf / div) % numModes;
		StateObjectCollection<OperatorStateHandle> s2 =
			mode == 0 ?
				StateObjectCollection.empty() :
				mode == 1 ?
					new StateObjectCollection<>(
						Collections.singletonList(createNewOperatorStateHandle(2, random))) :
					new StateObjectCollection<>(
						Arrays.asList(
							createNewOperatorStateHandle(2, random),
							createNewOperatorStateHandle(2, random)));

		div *= numModes;
		mode = (conf / div) % numModes;
		StateObjectCollection<KeyedStateHandle> s3 =
			mode == 0 ?
				StateObjectCollection.empty() :
				mode == 1 ?
					new StateObjectCollection<>(
						Collections.singletonList(createNewKeyedStateHandle(keyGroupRange))) :
					new StateObjectCollection<>(
						Arrays.asList(
							createNewKeyedStateHandle(keyGroupRange1),
							createNewKeyedStateHandle(keyGroupRange2)));

		div *= numModes;
		mode = (conf / div) % numModes;
		StateObjectCollection<KeyedStateHandle> s4 =
			mode == 0 ?
				StateObjectCollection.empty() :
				mode == 1 ?
					new StateObjectCollection<>(
						Collections.singletonList(createNewKeyedStateHandle(keyGroupRange))) :
					new StateObjectCollection<>(
						Arrays.asList(
							createNewKeyedStateHandle(keyGroupRange1),
							createNewKeyedStateHandle(keyGroupRange2)));

		return new OperatorSubtaskState(s1, s2, s3, s4);
	}

	/**
	 * For all 4 sub-states:
	 * - mode 0: One valid state handle (deep copy of original). Only this creates an OperatorSubtaskState that
	 *           qualifies as alternative.
	 * - mode 1: Empty StateHandleCollection.
	 * - mode 2: One invalid state handle (e.g. wrong key group, different meta data)
	 */
	private OperatorSubtaskState createAlternativeSubtaskState(OperatorSubtaskState primaryOriginal, int mode) {
		switch (mode) {
			case 0:
				return new OperatorSubtaskState(
					deepCopyFirstElement(primaryOriginal.getManagedOperatorState()),
					deepCopyFirstElement(primaryOriginal.getRawOperatorState()),
					deepCopyFirstElement(primaryOriginal.getManagedKeyedState()),
					deepCopyFirstElement(primaryOriginal.getRawKeyedState()));
			case 1:
				return new OperatorSubtaskState();
			case 2:
				KeyGroupRange otherRange = new KeyGroupRange(8, 16);
				int numNamedStates = 2;
				return new OperatorSubtaskState(
					createNewOperatorStateHandle(numNamedStates, random),
					createNewOperatorStateHandle(numNamedStates, random),
					createNewKeyedStateHandle(otherRange),
					createNewKeyedStateHandle(otherRange));
			default:
				throw new IllegalArgumentException("Mode: " + mode);
		}
	}

	private <T extends StateObject> boolean checkResultAsExpected(
		Function<OperatorSubtaskState, StateObjectCollection<T>> extractor,
		Function<PrioritizedOperatorSubtaskState, List<StateObjectCollection<T>>> extractor2,
		PrioritizedOperatorSubtaskState prioritizedResult,
		OperatorSubtaskState... expectedOrdered) {

		List<StateObjectCollection<T>> collector = new ArrayList<>(expectedOrdered.length);
		for (OperatorSubtaskState operatorSubtaskState : expectedOrdered) {
			collector.add(extractor.apply(operatorSubtaskState));
		}

		return checkRepresentSameOrder(
			extractor2.apply(prioritizedResult).iterator(),
			collector.toArray(new StateObjectCollection[collector.size()]));
	}

	private boolean checkRepresentSameOrder(
		Iterator<? extends StateObjectCollection<?>> ordered,
		StateObjectCollection<?>... expectedOrder) {

		for (StateObjectCollection<?> objects : expectedOrder) {
			if (!ordered.hasNext() || !checkContainedObjectsReferentialEquality(objects, ordered.next())) {
				return false;
			}
		}

		return !ordered.hasNext();
	}

	/**
	 * Returns true iff, in iteration order, all objects in the first collection are equal by reference to their
	 * corresponding object (by order) in the second collection and the size of the collections is equal.
	 */
	public boolean checkContainedObjectsReferentialEquality(StateObjectCollection<?> a, StateObjectCollection<?> b) {

		if (a == b) {
			return true;
		}

		if(a == null || b == null) {
			return false;
		}

		if (a.size() != b.size()) {
			return false;
		}

		Iterator<?> bIter = b.iterator();
		for (StateObject stateObject : a) {
			if (!bIter.hasNext() || bIter.next() != stateObject) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Creates a deep copy of the first state object in the given collection, or null if the collection is empy.
	 */
	private <T extends StateObject> T deepCopyFirstElement(StateObjectCollection<T> original) {
		if (original.isEmpty()) {
			return null;
		}

		T stateObject = original.iterator().next();
		StateObject result;
		if (stateObject instanceof OperatorStreamStateHandle) {
			result = deepDummyCopy((OperatorStateHandle) stateObject);
		} else if (stateObject instanceof KeyedStateHandle) {
			result = deepDummyCopy((KeyedStateHandle) stateObject);
		} else {
			throw new IllegalStateException();
		}
		return (T) result;
	}
}
