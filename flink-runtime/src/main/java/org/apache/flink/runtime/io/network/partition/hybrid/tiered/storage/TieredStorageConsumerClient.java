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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** {@link TieredStorageConsumerClient} is used to read buffer from tiered store. */
public class TieredStorageConsumerClient {

    private final List<TierConsumerAgent> tierConsumerAgents;

    public TieredStorageConsumerClient(List<TierFactory> tierFactories) {
        this.tierConsumerAgents = createTierConsumerAgents(tierFactories);
    }

    public void start() {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            tierConsumerAgent.start();
        }
    }

    public Optional<Buffer> getNextBuffer(int subpartitionId) {
        // TODO, the detailed logic will be completed when the memory tier is introduced..
        return Optional.empty();
    }

    public void close() throws IOException {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            tierConsumerAgent.close();
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Internal methods
    // --------------------------------------------------------------------------------------------

    private List<TierConsumerAgent> createTierConsumerAgents(List<TierFactory> tierFactories) {
        ArrayList<TierConsumerAgent> tierConsumerAgents = new ArrayList<>();
        for (TierFactory tierFactory : tierFactories) {
            tierConsumerAgents.add(tierFactory.createConsumerAgent());
        }
        return tierConsumerAgents;
    }
}
