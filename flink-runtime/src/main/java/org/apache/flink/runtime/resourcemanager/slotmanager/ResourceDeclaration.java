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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/** ResourceDeclaration for {@link ResourceAllocator}. */
public class ResourceDeclaration {
    private final WorkerResourceSpec spec;
    private final int numNeeded;

    /**
     * workers that {@link SlotManager} does not wanted. This is just a hint for {@link
     * ResourceAllocator} to decide which worker should be release.
     */
    private final Collection<InstanceID> unwantedWorkers;

    public ResourceDeclaration(
            WorkerResourceSpec spec, int numNeeded, Collection<InstanceID> unwantedWorkers) {
        this.spec = spec;
        this.numNeeded = numNeeded;
        this.unwantedWorkers = Collections.unmodifiableCollection(unwantedWorkers);
    }

    public WorkerResourceSpec getSpec() {
        return spec;
    }

    public int getNumNeeded() {
        return numNeeded;
    }

    public Collection<InstanceID> getUnwantedWorkers() {
        return unwantedWorkers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceDeclaration that = (ResourceDeclaration) o;
        return numNeeded == that.numNeeded
                && Objects.equals(spec, that.spec)
                && Objects.equals(unwantedWorkers, that.unwantedWorkers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(spec, numNeeded, unwantedWorkers);
    }

    @Override
    public String toString() {
        return "ResourceDeclaration{"
                + "spec="
                + spec
                + ", numNeeded="
                + numNeeded
                + ", unwantedWorkers="
                + unwantedWorkers
                + '}';
    }
}
