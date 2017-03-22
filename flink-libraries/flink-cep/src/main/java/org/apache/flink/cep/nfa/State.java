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

package org.apache.flink.cep.nfa;

import org.apache.flink.api.common.functions.FilterFunction;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Represents a state of the {@link NFA}.
 * <p>
 * Each state is identified by a name and a state type. Furthermore, it contains a collection of
 * state transitions. The state transitions describe under which conditions it is possible to enter
 * a new state.
 *
 * @param <T> Type of the input events
 */
public class State<T> implements Serializable {
	private static final long serialVersionUID = 6658700025989097781L;

	private final String name;
	private final StateType stateType;
	private final Collection<StateTransition<T>> stateTransitions;

	public State(final String name, final StateType stateType) {
		this.name = name;
		this.stateType = stateType;

		stateTransitions = new ArrayList<>();
	}

	public boolean isFinal() {
		return stateType == StateType.Final;
	}

	public boolean isStart() { return stateType == StateType.Start; }

	public String getName() {
		return name;
	}

	public Collection<StateTransition<T>> getStateTransitions() {
		return stateTransitions;
	}

	private void addStateTransition(
		final StateTransitionAction action,
		final State<T> targetState,
		final FilterFunction<T> condition) {
		stateTransitions.add(new StateTransition<T>(this, action, targetState, condition));
	}

	public void addIgnore(final FilterFunction<T> condition) {
		addStateTransition(StateTransitionAction.IGNORE, this, condition);
	}

	public void addIgnore(final State<T> targetState,final FilterFunction<T> condition) {
		addStateTransition(StateTransitionAction.IGNORE, targetState, condition);
	}

	public void addTake(final State<T> targetState, final FilterFunction<T> condition) {
		addStateTransition(StateTransitionAction.TAKE, targetState, condition);
	}

	public void addProceed(final State<T> targetState, final FilterFunction<T> condition) {
		addStateTransition(StateTransitionAction.PROCEED, targetState, condition);
	}

	public void addTake(final FilterFunction<T> condition) {
		addStateTransition(StateTransitionAction.TAKE, this, condition);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof State) {
			@SuppressWarnings("unchecked")
			State<T> other = (State<T>)obj;

			return name.equals(other.name) &&
				stateType == other.stateType &&
				stateTransitions.equals(other.stateTransitions);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		builder.append("State(").append(name).append(", ").append(stateType).append(", [\n");

		for (StateTransition<T> stateTransition: stateTransitions) {
			builder.append(stateTransition).append(",\n");
		}

		builder.append("])");

		return builder.toString();
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, stateType, stateTransitions);
	}

	/**
	 * Set of valid state types.
	 */
	public enum StateType {
		Start, // the state is a starting state for the NFA
		Final, // the state is a final state for the NFA
		Normal // the state is neither a start nor a final state
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();

		//Backward compatibility. Previous version of StateTransition did not have source state
		if (!stateTransitions.isEmpty() && stateTransitions.iterator().next().getSourceState() == null) {
			final List<StateTransition<T>> tmp = new ArrayList<>();
			tmp.addAll(this.stateTransitions);

			this.stateTransitions.clear();
			for (StateTransition<T> transition : tmp) {
				addStateTransition(transition.getAction(), transition.getTargetState(), transition.getCondition());
			}
		}
	}
}
