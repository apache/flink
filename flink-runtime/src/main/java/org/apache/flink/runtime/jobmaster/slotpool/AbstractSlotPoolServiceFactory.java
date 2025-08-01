/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.util.clock.Clock;

import javax.annotation.Nonnull;

import java.time.Duration;

/** Abstract SlotPoolServiceFactory. */
public abstract class AbstractSlotPoolServiceFactory implements SlotPoolServiceFactory {

    @Nonnull protected final Clock clock;

    @Nonnull protected final Duration rpcTimeout;

    @Nonnull protected final Duration slotIdleTimeout;

    @Nonnull protected final Duration batchSlotTimeout;

    @Nonnull protected final Duration slotRequestMaxInterval;

    protected final boolean deferSlotAllocation;

    protected AbstractSlotPoolServiceFactory(
            @Nonnull Clock clock,
            @Nonnull Duration rpcTimeout,
            @Nonnull Duration slotIdleTimeout,
            @Nonnull Duration batchSlotTimeout,
            @Nonnull Duration slotRequestMaxInterval,
            boolean deferSlotAllocation) {
        this.clock = clock;
        this.rpcTimeout = rpcTimeout;
        this.slotIdleTimeout = slotIdleTimeout;
        this.batchSlotTimeout = batchSlotTimeout;
        this.slotRequestMaxInterval = slotRequestMaxInterval;
        this.deferSlotAllocation = deferSlotAllocation;
    }
}
