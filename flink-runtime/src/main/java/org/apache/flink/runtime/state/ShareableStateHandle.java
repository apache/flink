/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;

/**
 * A handle to a state shared across multiple execution units, such as Subtasks or Operators. It is
 * not supposed to be extended by {@link CompositeStateHandle}: if the latter does have shared state
 * then such state can be exposed by {@link #accept(StateObjectVisitor) accepting a visitor}.
 */
@Internal
public interface ShareableStateHandle extends StateObject {

    /** @return {@link StateObjectID ID} of this object, such as file name on DFS. */
    StateObjectID getID();

    /** @return true if the referenced state is actually shared in the current deployment. */
    boolean isShared();

    /**
     * @return a new state handle pointing to the same state object but explicitly marked as shared,
     *     i.e. it will return true from {@link #isShared()}.
     */
    StateObject asShared();
}
