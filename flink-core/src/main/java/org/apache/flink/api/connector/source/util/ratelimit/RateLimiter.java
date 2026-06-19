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

package org.apache.flink.api.connector.source.util.ratelimit;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceSplit;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.concurrent.CompletionStage;

/**
 * The interface to rate limit execution of methods.
 *
 * @param <SplitT> The type of the source splits.
 */
@NotThreadSafe
@PublicEvolving
public interface RateLimiter<SplitT extends SourceSplit> {

    /**
     * Returns a future that is completed once another event would not exceed the rate limit. For
     * correct functioning, the next invocation of this method should only happen after the
     * previously returned future has been completed.
     */
    default CompletionStage<Void> acquire() {
        return acquire(1);
    }

    /**
     * Returns a future that is completed once other events would not exceed the rate limit. For
     * correct functioning, the next invocation of this method should only happen after the
     * previously returned future has been completed.
     *
     * @param numberOfEvents The number of events.
     */
    CompletionStage<Void> acquire(int numberOfEvents);

    /**
     * Notifies this {@code RateLimiter} that the checkpoint with the given {@code checkpointId}
     * completed and was committed. Makes it possible to implement rate limiters that control data
     * emission per checkpoint cycle.
     *
     * @param checkpointId The ID of the checkpoint that has been completed.
     */
    default void notifyCheckpointComplete(long checkpointId) {}

    /**
     * Notifies this {@code RateLimiter} that a new split has been added. For correct functioning,
     * this method should only be invoked after the returned future of previous {@link
     * #acquire(int)} method invocation has been completed.
     *
     * @param split The split that has been added.
     */
    default void notifyAddingSplit(SplitT split) {}
}
