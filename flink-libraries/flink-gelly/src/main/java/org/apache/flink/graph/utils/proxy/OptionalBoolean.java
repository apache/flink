/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.utils.proxy;

import org.apache.flink.graph.GraphAlgorithm;

/**
 * A multi-state boolean.
 *
 * <p>This class is used by {@link GraphAlgorithm} configuration options to set a default value
 * which can be overwritten. The default value is also used when algorithm configurations are merged
 * and conflict.
 */
public class OptionalBoolean {

    /** States for {@link OptionalBoolean}. */
    protected enum State {
        UNSET,
        FALSE,
        TRUE,
        CONFLICTING
    }

    private State state = State.UNSET;

    private final boolean valueIfUnset;

    private final boolean valueIfConflicting;

    /**
     * An {@code OptionalBoolean} has three possible states: true, false, and "unset". The value is
     * set when merged with a value of true or false. The state returns to unset either explicitly
     * or when true is merged with false.
     *
     * @param valueIfUnset the value to return when the object's state is unset
     * @param valueIfConflicting the value to return when the object's state is conflicting
     */
    public OptionalBoolean(boolean valueIfUnset, boolean valueIfConflicting) {
        this.valueIfUnset = valueIfUnset;
        this.valueIfConflicting = valueIfConflicting;
    }

    /**
     * Get the boolean state.
     *
     * @return boolean state
     */
    public boolean get() {
        switch (state) {
            case UNSET:
                return valueIfUnset;
            case FALSE:
                return false;
            case TRUE:
                return true;
            case CONFLICTING:
                return valueIfConflicting;
            default:
                throw new RuntimeException("Unknown state");
        }
    }

    /**
     * Set the boolean state.
     *
     * @param value boolean state
     */
    public void set(boolean value) {
        this.state = (value ? State.TRUE : State.FALSE);
    }

    /** Reset to the unset state. */
    public void unset() {
        this.state = State.UNSET;
    }

    /**
     * Get the actual state.
     *
     * @return actual state
     */
    protected State getState() {
        return state;
    }

    /**
     * The conflicting states are true with false and false with true.
     *
     * @param other object to test with
     * @return whether the objects conflict
     */
    public boolean conflictsWith(OptionalBoolean other) {
        return state == State.CONFLICTING
                || other.state == State.CONFLICTING
                || (state == State.TRUE && other.state == State.FALSE)
                || (state == State.FALSE && other.state == State.TRUE);
    }

    /**
     * State transitions. - if the states are the same then no change - if either state is unset
     * then change to the other state - if the states are conflicting then set to the conflicting
     * state
     *
     * @param other object from which to merge state
     */
    public void mergeWith(OptionalBoolean other) {
        if (state == other.state) {
            // no change in state
        } else if (state == State.UNSET) {
            state = other.state;
        } else if (other.state == State.UNSET) {
            // no change in state
        } else {
            state = State.CONFLICTING;
        }
    }
}
