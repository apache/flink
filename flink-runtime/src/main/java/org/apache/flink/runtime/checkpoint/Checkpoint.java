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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.util.concurrent.FutureUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** A checkpoint, pending or completed. */
public interface Checkpoint {
    DiscardObject NOOP_DISCARD_OBJECT = () -> {};

    long getCheckpointID();

    /**
     * This method precede the {@link DiscardObject#discard()} method and should be called from the
     * {@link CheckpointCoordinator}(under the lock) while {@link DiscardObject#discard()} can be
     * called from any thread/place.
     */
    DiscardObject markAsDiscarded();

    /** Extra interface for discarding the checkpoint. */
    interface DiscardObject {
        void discard() throws Exception;

        default CompletableFuture<Void> discardAsync(Executor ioExecutor) {
            return FutureUtils.runAsync(this::discard, ioExecutor);
        }
    }
}
