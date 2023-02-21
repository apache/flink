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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;

/**
 * Interface for shuffle service factory implementations.
 *
 * <p>This component is a light-weight factory for {@link ShuffleMaster} and {@link
 * ShuffleEnvironment}.
 *
 * @param <SD> partition shuffle descriptor used for producer/consumer deployment and their data
 *     exchange.
 * @param <P> type of provided result partition writers
 * @param <G> type of provided input gates
 */
public interface ShuffleServiceFactory<
        SD extends ShuffleDescriptor, P extends ResultPartitionWriter, G extends IndexedInputGate> {

    /**
     * Factory method to create a specific {@link ShuffleMaster} implementation.
     *
     * @param shuffleMasterContext shuffle context for shuffle master.
     * @return shuffle manager implementation
     */
    ShuffleMaster<SD> createShuffleMaster(ShuffleMasterContext shuffleMasterContext);

    /**
     * Factory method to create a specific local {@link ShuffleEnvironment} implementation.
     *
     * @param shuffleEnvironmentContext local context
     * @return local shuffle service environment implementation
     */
    ShuffleEnvironment<P, G> createShuffleEnvironment(
            ShuffleEnvironmentContext shuffleEnvironmentContext);
}
