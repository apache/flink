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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.SubEvent;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.State;
import org.apache.flink.cep.nfa.StateTransition;
import org.apache.flink.cep.nfa.StateTransitionAction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class NFACompilerTest extends TestLogger {

	/**
	 * Tests that the NFACompiler generates the correct NFA from a given Pattern
	 */
	@Test
	public void testNFACompilerWithSimplePattern() {
		Pattern<Event, Event> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 3314714776170474221L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getPrice() > 2;
			}
		})
		.followedBy("middle").subtype(SubEvent.class)
		.next("end").where(new FilterFunction<Event>() {
				private static final long serialVersionUID = 3990995859716364087L;

				@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		TypeInformation<Event> typeInformation = (TypeInformation<Event>) TypeExtractor.createTypeInfo(Event.class);

		NFA<Event> nfa = NFACompiler.<Event>compile(pattern, typeInformation.createSerializer(new ExecutionConfig()));

		Set<State<Event>> states = nfa.getStates();

		assertEquals(4, states.size());

		Map<String, State<Event>> stateMap = new HashMap<>();

		for (State<Event> state: states) {
			stateMap.put(state.getName(), state);
		}

		assertTrue(stateMap.containsKey(NFACompiler.BEGINNING_STATE_NAME));
		State<Event> beginningState = stateMap.get(NFACompiler.BEGINNING_STATE_NAME);

		assertTrue(beginningState.isStart());

		assertTrue(stateMap.containsKey("start"));
		State<Event> startState = stateMap.get("start");

		Collection<StateTransition<Event>> startTransitions = startState.getStateTransitions();
		Map<String, StateTransition<Event>> startTransitionMap = new HashMap<>();

		for (StateTransition<Event> transition: startTransitions) {
			startTransitionMap.put(transition.getTargetState().getName(), transition);
		}

		assertEquals(2, startTransitionMap.size());
		assertTrue(startTransitionMap.containsKey("start"));

		StateTransition<Event> reflexiveTransition = startTransitionMap.get("start");
		assertEquals(StateTransitionAction.IGNORE, reflexiveTransition.getAction());

		assertTrue(startTransitionMap.containsKey("middle"));
		StateTransition<Event> startMiddleTransition = startTransitionMap.get("middle");
		assertEquals(StateTransitionAction.TAKE, startMiddleTransition.getAction());

		assertTrue(stateMap.containsKey("middle"));
		State<Event> middleState = stateMap.get("middle");

		Map<String, StateTransition<Event>> middleTransitionMap = new HashMap<>();

		for (StateTransition<Event> transition: middleState.getStateTransitions()) {
			middleTransitionMap.put(transition.getTargetState().getName(), transition);
		}

		assertEquals(1, middleTransitionMap.size());

		assertTrue(middleTransitionMap.containsKey("end"));
		StateTransition<Event> middleEndTransition = middleTransitionMap.get("end");

		assertEquals(StateTransitionAction.TAKE, middleEndTransition.getAction());

		assertTrue(stateMap.containsKey("end"));
		State<Event> endState = stateMap.get("end");

		assertTrue(endState.isFinal());
		assertEquals(0, endState.getStateTransitions().size());
	}
}
