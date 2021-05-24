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

package org.apache.flink.state.api.output;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;

import java.util.Objects;

/** A simple wrapper pojo that tags {@link OperatorSubtaskState} with metadata. */
@Internal
@SuppressWarnings("WeakerAccess")
public final class TaggedOperatorSubtaskState {

    public int index;

    public OperatorSubtaskState state;

    public TaggedOperatorSubtaskState(int index, OperatorSubtaskState state) {
        this.index = index;
        this.state = state;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TaggedOperatorSubtaskState that = (TaggedOperatorSubtaskState) o;
        return index == that.index && Objects.equals(state, that.state);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, state);
    }

    @Override
    public String toString() {
        return "TaggedOperatorSubtaskState{" + "index=" + index + ", state=" + state + '}';
    }
}
