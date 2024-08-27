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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;

/** Context on the {@link StreamTask} for figuring out whether it has been cancelled. */
@FunctionalInterface
@Internal
public interface StreamTaskCancellationContext {

    /**
     * Factory for a context that always returns {@code false} when {@link #isCancelled()} is
     * called.
     *
     * @return context
     */
    static StreamTaskCancellationContext alwaysRunning() {
        return () -> false;
    }

    /**
     * Find out whether the {@link StreamTask} this context belongs to has been cancelled.
     *
     * @return true if the {@code StreamTask} the has been cancelled
     */
    boolean isCancelled();
}
