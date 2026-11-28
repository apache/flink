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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageDataIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A registry that maintains local or remote resources that correspond to a certain set of data in
 * the Tiered Storage.
 *
 * <p>The registry is shared by all result partitions of a TaskManager: resources are registered
 * while a partition is being set up, and released once the partition is dropped. Registration and
 * removal may therefore happen concurrently.
 *
 * <p>Registration and removal are linearized per owner. A resource is released at most once, by the
 * first removal that observes it; a resource that is registered after a removal has taken effect
 * starts a new generation and is released only by a later removal of the same owner. The owner is
 * removed from the map before its resources are released, so a registration can never be appended
 * to a list that is already being released.
 *
 * <p>Consequently, the registry cannot guarantee that every registered resource is released: a
 * resource registered after the last removal of its owner is never released. Callers must ensure
 * that all registrations for an owner happen before the final removal of that owner. The tiered
 * storage lifecycle satisfies this, since resources are registered while the result partition is
 * created and a partition can only be released after it has been set up.
 */
public class TieredStorageResourceRegistry {

    // Mutated only inside the map's atomic operations, so a plain ArrayList is sufficient.
    private final ConcurrentHashMap<TieredStorageDataIdentifier, List<TieredStorageResource>>
            registeredResources = new ConcurrentHashMap<>();

    /**
     * Register a new resource for the given owner.
     *
     * @param owner identifier of the data that the resource corresponds to.
     * @param tieredStorageResource the tiered storage resources to be registered.
     */
    public void registerResource(
            TieredStorageDataIdentifier owner, TieredStorageResource tieredStorageResource) {
        registeredResources.compute(
                owner,
                (ignore, resources) -> {
                    if (resources == null) {
                        resources = new ArrayList<>();
                    }
                    resources.add(tieredStorageResource);
                    return resources;
                });
    }

    /**
     * Remove all resources for the given owner.
     *
     * @param owner identifier of the data that the resources correspond to.
     */
    public void clearResourceFor(TieredStorageDataIdentifier owner) {
        List<TieredStorageResource> cleanersForOwner = registeredResources.remove(owner);

        if (cleanersForOwner != null) {
            cleanersForOwner.forEach(TieredStorageResource::release);
        }
    }
}
