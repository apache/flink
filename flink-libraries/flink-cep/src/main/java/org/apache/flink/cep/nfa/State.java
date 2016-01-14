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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

public class State<T> implements Serializable {
	private static final long serialVersionUID = 6658700025989097781L;

	private final String name;
	private final StateType stateType;
	private final Collection<StateTransition<T>> stateTransitions;

	public State(final String name, final StateType stateType) {
		this.name = name;
		this.stateType = stateType;

		stateTransitions = new ArrayList<StateTransition<T>>();
	}

	public boolean isFinal() {
		return stateType == StateType.Final;
	}

	public boolean isStart() { return stateType == StateType.Start; }

	public String getName() {
		return name;
	}

	public StateType getStateType() {
		return stateType;
	}

	public Collection<StateTransition<T>> getStateTransitions() {
		return stateTransitions;
	}

	public void addStateTransition(final StateTransition<T> stateTransition) {
		stateTransitions.add(stateTransition);
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

	public enum StateType {
		Start,
		Final,
		Normal
	}
}
