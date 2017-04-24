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
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.io.Serializable;
import java.util.Objects;

public class StateTransition<T> implements Serializable {
	private static final long serialVersionUID = -4825345749997891838L;

	private final StateTransitionAction action;
	private final State<T> sourceState;
	private final State<T> targetState;
	private IterativeCondition<T> newCondition;

	/**
	 * @deprecated 	This field remains for backwards compatibility.
	 * Now the conditions extend the {@link IterativeCondition}.
	 */
	@Deprecated
	private FilterFunction<T> condition;

	public StateTransition(
			final State<T> sourceState,
			final StateTransitionAction action,
			final State<T> targetState,
			final IterativeCondition<T> condition) {
		this.action = action;
		this.targetState = targetState;
		this.sourceState = sourceState;
		this.newCondition = condition;
	}

	public StateTransitionAction getAction() {
		return action;
	}

	public State<T> getTargetState() {
		return targetState;
	}

	public State<T> getSourceState() {
		return sourceState;
	}

	public IterativeCondition<T> getCondition() {
		if (condition != null) {
			this.newCondition = new FilterWrapper<>(condition);
			this.condition = null;
		}
		return newCondition;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof StateTransition) {
			@SuppressWarnings("unchecked")
			StateTransition<T> other = (StateTransition<T>) obj;

			return action == other.action &&
				sourceState.getName().equals(other.sourceState.getName()) &&
				targetState.getName().equals(other.targetState.getName());
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		// we have to take the name of targetState because the transition might be reflexive
		return Objects.hash(action, targetState.getName(), sourceState.getName());
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		builder.append("StateTransition(")
			.append(action).append(", ")
			.append(sourceState.getName()).append(", ")
			.append(targetState.getName());

		if (newCondition != null) {
			builder.append(", with filter)");
		} else {
			builder.append(")");
		}

		return builder.toString();
	}

	/**
	 * A wrapper to transform a {@link FilterFunction} into a {@link SimpleCondition}.
	 * This is used only when migrating from an older Flink version.
	 */
	private static class FilterWrapper<T> extends SimpleCondition<T> {

		private static final long serialVersionUID = -4973016745698940430L;

		private final FilterFunction<T> filterFunction;

		FilterWrapper(FilterFunction<T> filterFunction) {
			this.filterFunction = filterFunction;
		}

		@Override
		public boolean filter(T value) throws Exception {
			return filterFunction.filter(value);
		}
	}
}
