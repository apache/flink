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

package org.apache.flink.streaming.runtime.operators.sink.committables;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

import java.io.IOException;
import java.util.Collection;

/**
 * Internal wrapper to handle the committing of committables.
 *
 * @param <CommT> type of the committable
 */
@Internal
public interface CommittableManager<CommT> {
    /** Returns a summary of the current commit progress. */
    CommittableSummary<CommT> getSummary();

    /**
     * Commits all due committables.
     *
     * @param fullyReceived only commit committables if all committables of this checkpoint for a
     *     subtask are received
     * @param committer used to commit to the external system
     * @return successfully committed committables with meta information
     * @throws IOException
     * @throws InterruptedException
     */
    Collection<CommittableWithLineage<CommT>> commit(
            boolean fullyReceived, Committer<CommT> committer)
            throws IOException, InterruptedException;
}
