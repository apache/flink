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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TierFactoryInitializer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;

import java.util.List;

/** Configurations for the Tiered Storage. */
public class TieredStorageConfiguration {

    private final List<TierFactory> tierFactories;

    private TieredStorageConfiguration(List<TierFactory> tierFactories) {
        this.tierFactories = tierFactories;
    }

    public List<TierFactory> getTierFactories() {
        return tierFactories;
    }

    public static TieredStorageConfiguration fromConfiguration(Configuration configuration) {
        return new TieredStorageConfiguration(
                TierFactoryInitializer.initializeTierFactories(configuration));
    }
}
