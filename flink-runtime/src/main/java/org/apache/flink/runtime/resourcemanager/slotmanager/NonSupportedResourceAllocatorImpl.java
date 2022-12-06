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

/** ResourceAllocator that not support to allocate/release resources. */
public class NonSupportedResourceAllocatorImpl implements ResourceAllocator {
    public static final NonSupportedResourceAllocatorImpl INSTANCE =
            new NonSupportedResourceAllocatorImpl();

    private NonSupportedResourceAllocatorImpl() {}

    @Override
    public boolean isSupported() {
        return false;
    }

    @Override
    public void releaseResource(InstanceID instanceId, Exception cause) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void releaseResource(ResourceID resourceID) {
        throw new UnsupportedOperationException();
    }

    public void allocateResource(WorkerResourceSpec workerResourceSpec) {
        throw new UnsupportedOperationException();
    }
}
