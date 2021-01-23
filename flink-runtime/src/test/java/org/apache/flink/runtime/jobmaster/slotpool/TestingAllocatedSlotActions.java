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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.function.Consumer;

/** Simple {@link AllocatedSlotActions} implementations for testing purposes. */
public class TestingAllocatedSlotActions implements AllocatedSlotActions {

    private volatile Consumer<Tuple2<SlotRequestId, Throwable>> releaseSlotConsumer;

    public void setReleaseSlotConsumer(
            @Nullable Consumer<Tuple2<SlotRequestId, Throwable>> releaseSlotConsumer) {
        this.releaseSlotConsumer = releaseSlotConsumer;
    }

    @Override
    public void releaseSlot(@Nonnull SlotRequestId slotRequestId, @Nullable Throwable cause) {
        Consumer<Tuple2<SlotRequestId, Throwable>> currentReleaseSlotConsumer =
                this.releaseSlotConsumer;

        if (currentReleaseSlotConsumer != null) {
            currentReleaseSlotConsumer.accept(Tuple2.of(slotRequestId, cause));
        }
    }
}
