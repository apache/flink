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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;

import java.util.Collection;

/** Resource related actions which the {@link SlotManager} can perform. */
public interface ResourceAllocator {

    /** Whether allocate/release resources are supported. */
    boolean isSupported();

    /**
     * Releases the resource with the given instance id.
     *
     * @param instanceId identifying which resource to release
     * @param cause why the resource is released
     */
    void releaseResource(InstanceID instanceId, Exception cause);

    /**
     * Clean up the disconnected resource with the given resource id.
     *
     * @param resourceID identifying which resource to clean up
     */
    void cleaningUpDisconnectedResource(ResourceID resourceID);

    /**
     * Requests to allocate a resource with the given {@link WorkerResourceSpec}.
     *
     * @param workerResourceSpec for the to be allocated worker
     */
    void allocateResource(WorkerResourceSpec workerResourceSpec);

    /** declare resource need by slot manager. */
    void declareResourceNeeded(Collection<ResourceDeclaration> resourceDeclarations);
}
