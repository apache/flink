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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.clock.Clock;

import javax.annotation.Nonnull;

/** Factory for the {@link DeclarativeSlotPoolService}. */
public class DeclarativeSlotPoolServiceFactory implements SlotPoolServiceFactory {

    private final Clock clock;
    private final Time idleSlotTimeout;
    private final Time rpcTimeout;

    public DeclarativeSlotPoolServiceFactory(Clock clock, Time idleSlotTimeout, Time rpcTimeout) {
        this.clock = clock;
        this.idleSlotTimeout = idleSlotTimeout;
        this.rpcTimeout = rpcTimeout;
    }

    @Nonnull
    @Override
    public SlotPoolService createSlotPoolService(@Nonnull JobID jobId) {
        return new DeclarativeSlotPoolService(
                jobId, new DefaultDeclarativeSlotPoolFactory(), clock, idleSlotTimeout, rpcTimeout);
    }
}
