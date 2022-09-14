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

package org.apache.flink.streaming.examples.statemachine.dfa;

import org.apache.flink.streaming.examples.statemachine.event.EventType;

import java.util.Random;

/**
 * The State captures the main functionality of the state machine. It represents a specific state in
 * the state machine, and holds all transitions possible from a specific state.
 *
 * <p>The state transition diagram is as follows:
 *
 * <pre>
 *           +--[a]--> W --[b]--> Y --[e]---+
 *           |                    ^         |
 *   Initial-+                    |         |
 *           |                    |         +--> (Z)-----[g]---> Terminal
 *           +--[c]--> X --[b]----+         |
 *                     |                    |
 *                     +--------[d]---------+
 * </pre>
 */
public enum State {

    /** The terminal state in the state machine. */
    Terminal,

    /**
     * Special state returned by the State.transition(...) function when attempting an illegal state
     * transition.
     */
    InvalidTransition,

    /** State 'Z'. */
    Z(new Transition(EventType.g, Terminal, 1.0f)),

    /** State 'Y'. */
    Y(new Transition(EventType.e, Z, 1.0f)),

    /** State 'X'. */
    X(new Transition(EventType.b, Y, 0.2f), new Transition(EventType.d, Z, 0.8f)),

    /** State 'W'. */
    W(new Transition(EventType.b, Y, 1.0f)),

    /** The initial state from which all state sequences start. */
    Initial(new Transition(EventType.a, W, 0.6f), new Transition(EventType.c, X, 0.4f));

    // ------------------------------------------------------------------------

    private final Transition[] transitions;

    State(Transition... transitions) {
        this.transitions = transitions;
    }

    /** Checks if this state is a terminal state. A terminal state has no outgoing transitions. */
    public boolean isTerminal() {
        return transitions.length == 0;
    }

    // ------------------------------------------------------------------------

    /**
     * Gets the state after transitioning from this state based on the given event. If the
     * transition is valid, this returns the new state, and if this transition is illegal, it
     * returns [[InvalidTransition]].
     *
     * @param evt The event that defined the transition.
     * @return The new state, or [[InvalidTransition]].
     */
    public State transition(EventType evt) {
        for (Transition t : transitions) {
            if (t.eventType() == evt) {
                return t.targetState();
            }
        }

        // no transition found
        return InvalidTransition;
    }

    /**
     * Picks a random transition, based on the probabilities of the outgoing transitions of this
     * state.
     *
     * @param rnd The random number generator to use.
     * @return A pair of (transition event , new state).
     */
    public EventTypeAndState randomTransition(Random rnd) {
        if (isTerminal()) {
            throw new RuntimeException("Cannot transition from state " + name());
        } else {
            final float p = rnd.nextFloat();
            float mass = 0.0f;
            Transition transition = null;

            for (Transition t : transitions) {
                mass += t.prob();
                if (p <= mass) {
                    transition = t;
                    break;
                }
            }

            assert transition != null;
            return new EventTypeAndState(transition.eventType(), transition.targetState());
        }
    }

    /**
     * Returns an event type that, if applied as a transition on this state, will result in an
     * illegal state transition.
     *
     * @param rnd The random number generator to use.
     * @return And event type for an illegal state transition.
     */
    public EventType randomInvalidTransition(Random rnd) {
        while (true) {
            EventType candidate = EventType.values()[rnd.nextInt(EventType.values().length)];
            if (transition(candidate) == InvalidTransition) {
                return candidate;
            }
        }
    }
}
