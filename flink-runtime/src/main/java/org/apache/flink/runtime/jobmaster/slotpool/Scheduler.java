/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmaster.SlotOwner;

import javax.annotation.Nonnull;

/**
 * Basic interface for the current scheduler, which is a {@link SlotProvider} and a {@link
 * SlotOwner}.
 */
public interface Scheduler extends SlotProvider, SlotOwner {

    /**
     * Start the scheduler by initializing the main thread executor.
     *
     * @param mainThreadExecutor the main thread executor of the job master.
     */
    void start(@Nonnull ComponentMainThreadExecutor mainThreadExecutor);

    /**
     * Returns true, iff the scheduling strategy of the scheduler requires to know about previous
     * allocations.
     */
    boolean requiresPreviousExecutionGraphAllocations();
}
