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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.function.ThrowingConsumer;

import java.io.IOException;

/**
 * Logs changes to a state created by {@link ChangelogKeyedStateBackend}. The changes are intended
 * to be stored durably, included into a checkpoint and replayed on recovery in case of failure.
 *
 * <p>Not that the order of updating the delegated state and logging it using this class usually
 * doesn't matter. However in some cases an already updated state needs to be logged. Besides that,
 * delegated state update is usually local and would fail faster. Therefore, consider updating the
 * delegated state first and logging the change second.
 *
 * <p>If state update succeeds and changelog append fails immediately then the updated change is
 * discarded as no checkpoints can proceed.
 *
 * <p>If changelog append fails asynchronously then subsequent checkpoints can only succeed after
 * state materialization.
 *
 * @param <Value> type of state (value)
 * @param <Namespace> type of namespace
 */
interface StateChangeLogger<Value, Namespace> {

    /** State updated, such as by {@link ListState#update}. */
    void valueUpdated(Value newValue, Namespace ns) throws IOException;

    /** State update internally (usually with a value that the user has no control over). */
    void valueUpdatedInternal(Value newValue, Namespace ns) throws IOException;

    /** State added, such as append to list.addAll. */
    void valueAdded(Value addedValue, Namespace ns) throws IOException;

    /** State cleared. */
    void valueCleared(Namespace ns) throws IOException;

    /** State element added, such as append of a single element to a list. */
    void valueElementAdded(
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataSerializer, Namespace ns)
            throws IOException;

    /** State element added or updated, such as put into a map. */
    void valueElementAddedOrUpdated(
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataSerializer, Namespace ns)
            throws IOException;

    /** State element removed, such mapping removal from a map. */
    void valueElementRemoved(
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataSerializer, Namespace ns)
            throws IOException;
}
