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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.PublicEvolving;

/**
 * An decorative interface of {@link SplitEnumerator} which allows to handle {@link SourceEvent}
 * sent from a specific execution attempt.
 *
 * <p>The split enumerator must implement this interface if it needs to deal with custom source
 * events and is used in cases that a subtask can have multiple concurrent execution attempts, e.g.
 * if speculative execution is enabled. Otherwise an error will be thrown when the split enumerator
 * receives a custom source event.
 */
@PublicEvolving
public interface SupportsHandleExecutionAttemptSourceEvent {

    /**
     * Handles a custom source event from the source reader. It is similar to {@link
     * SplitEnumerator#handleSourceEvent(int, SourceEvent)} but is aware of the subtask execution
     * attempt who sent this event.
     *
     * @param subtaskId the subtask id of the source reader who sent the source event.
     * @param attemptNumber the attempt number of the source reader who sent the source event.
     * @param sourceEvent the source event from the source reader.
     */
    void handleSourceEvent(int subtaskId, int attemptNumber, SourceEvent sourceEvent);
}
