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

package org.apache.flink.streaming.examples.statemachine.event;

import org.apache.flink.streaming.examples.statemachine.dfa.State;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Data type for alerts. */
public class Alert {

    private final int address;

    private final State state;

    private final EventType transition;

    /**
     * Creates a new alert.
     *
     * @param address The originating address (think 32 bit IPv4 address).
     * @param state The state that the event state machine found.
     * @param transition The transition that was considered invalid.
     */
    public Alert(int address, State state, EventType transition) {
        this.address = address;
        this.state = checkNotNull(state);
        this.transition = checkNotNull(transition);
    }

    // ------------------------------------------------------------------------

    public int address() {
        return address;
    }

    public State state() {
        return state;
    }

    public EventType transition() {
        return transition;
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        int code = 31 * address + state.hashCode();
        return 31 * code + transition.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        } else {
            final Alert that = (Alert) obj;
            return this.address == that.address
                    && this.transition == that.transition
                    && this.state == that.state;
        }
    }

    @Override
    public String toString() {
        return "ALERT "
                + Event.formatAddress(address)
                + " : "
                + state.name()
                + " -> "
                + transition.name();
    }
}
