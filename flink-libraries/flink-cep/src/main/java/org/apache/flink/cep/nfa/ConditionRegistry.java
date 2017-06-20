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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The {@link ConditionRegistry} is a registry to manage the mapping from a {@link StateTransition}
 * to the {@link IterativeCondition}. A {@link StateTransition} is unique in a {@link NFA}, that's
 * why we use {@link StateTransition} as the key.
 */
public class ConditionRegistry implements Serializable {
	private static final long serialVersionUID = -4130291010425184830L;

	private final Map<TransitionInfo, IterativeCondition<?>> registeredConditions;

	private final TransitionInfo reuse = new TransitionInfo();

	/**
	 * Creates a new condition registry.
	 */
	public ConditionRegistry() {
		this.registeredConditions = new HashMap<>();
	}

	/**
	 * Registers an {@link IterativeCondition} for the {@link StateTransition}.
	 * @param transition the state transition
	 * @param condition the condition to register, maybe null
	 */
	public <T> void registerCondition(StateTransition<T> transition, IterativeCondition<T> condition) {
		registeredConditions.put(new TransitionInfo(transition), condition);
	}

	/**
	 * Gets the corresponding {@link IterativeCondition} with the given {@link StateTransition}.
	 * @param transition the state transition
	 * @return the corresponding {@link IterativeCondition}, maybe null.
	 */
	public <T> IterativeCondition<T> getCondition(StateTransition<T> transition) {
		reuse.update(transition);
		//noinspection unchecked
		return (IterativeCondition<T>) registeredConditions.get(reuse);
	}

	/**
	 * Opens every non-null conditions.
	 */
	public void open() throws Exception {
		for (IterativeCondition<?> condition : registeredConditions.values()) {
			if (condition != null) {
				FunctionUtils.openFunction(condition, new Configuration());
			}
		}
	}

	/**
	 * Sets the RuntimeContext for every non-null conditions.
	 * @param context the runtime context
	 */
	public void setRuntimeContext(RuntimeContext context) {
		for (IterativeCondition<?> condition : registeredConditions.values()) {
			if (condition != null) {
				FunctionUtils.setFunctionRuntimeContext(condition, context);
			}
		}
	}

	private static class TransitionInfo implements Serializable {
		private static final long serialVersionUID = -6446693486080356589L;

		private StateTransitionAction action;
		private String sourceStateName;
		private String targetStateName;

		TransitionInfo(){
		}

		TransitionInfo(StateTransition<?> transition) {
			this.action = transition.getAction();
			this.sourceStateName = transition.getSourceState().getName();
			this.targetStateName = transition.getTargetState().getName();
		}

		void update(StateTransition<?> transition) {
			this.action = transition.getAction();
			this.sourceStateName = transition.getSourceState().getName();
			this.targetStateName = transition.getTargetState().getName();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof TransitionInfo) {
				@SuppressWarnings("unchecked")
				TransitionInfo other = (TransitionInfo) obj;

				return action == other.action &&
						sourceStateName.equals(other.sourceStateName) &&
						targetStateName.equals(other.targetStateName);
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			// we have to take the name of targetState because the transition might be reflexive
			return Objects.hash(action, targetStateName, sourceStateName);
		}
	}
}
