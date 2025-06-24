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

package org.apache.flink.runtime.messages;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/** Response to the request to collect thread details samples. */
public class TaskThreadInfoResponse implements Serializable {

    private static final long serialVersionUID = -4786454630050578031L;

    private final Map<ExecutionAttemptID, Collection<ThreadInfoSample>> samples;

    /**
     * Creates a response to the request to collect thread details samples.
     *
     * @param samples Thread info samples.
     */
    public TaskThreadInfoResponse(Map<ExecutionAttemptID, Collection<ThreadInfoSample>> samples) {
        this.samples = Preconditions.checkNotNull(samples);
    }

    /**
     * Returns a collection of ThreadInfoSample.
     *
     * @return A collection of thread info samples for a particular execution attempt (Task)
     */
    public Map<ExecutionAttemptID, Collection<ThreadInfoSample>> getSamples() {
        return samples;
    }
}
