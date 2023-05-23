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

/**
 * The memory specs for a memory owner, including the owner itself, the number of guaranteed buffers
 * of the memory owner, etc.
 */
public class TieredStorageMemorySpec {

    /** The memory use owner. */
    private final Object owner;

    /** The number of guaranteed buffers of this memory owner. */
    private final int numGuaranteedBuffers;

    public TieredStorageMemorySpec(Object owner, int numGuaranteedBuffers) {
        this.owner = owner;
        this.numGuaranteedBuffers = numGuaranteedBuffers;
    }

    public Object getOwner() {
        return owner;
    }

    public int getNumGuaranteedBuffers() {
        return numGuaranteedBuffers;
    }
}
