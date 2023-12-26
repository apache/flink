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
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.LocalBufferPool;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;

import java.util.List;

/**
 * The {@link TieredStorageMemoryManager} is to request or recycle buffers from {@link
 * LocalBufferPool} for different memory owners, for example, the tiers, the buffer accumulator,
 * etc. Note that the logic for requesting and recycling buffers is consistent for these owners.
 *
 * <p>The buffers managed by {@link TieredStorageMemoryManager} is categorized into two types:
 * <b>non-reclaimable</b> buffers which cannot be immediately released and <b>reclaimable
 * buffers</b> which can be reclaimed quickly and safely. Non-reclaimable buffers necessitates
 * waiting for other operations to complete before releasing it, such as downstream consumption. On
 * the other hand, reclaimable buffers can be freed up at any time, enabling rapid memory recycling
 * for tasks such as flushing memory to disk or remote storage.
 *
 * <p>The {@link TieredStorageMemoryManager} does not provide strict memory limitations on any user
 * can request. Instead, it only simply provides memory usage hints to memory users. It is very
 * <b>important</b> to note that <b>only</b> users with non-reclaimable should check the memory
 * hints by calling {@code getMaxNonReclaimableBuffers} before requesting buffers.
 */
public interface TieredStorageMemoryManager {

    /**
     * Setup the {@link TieredStorageMemoryManager}. When setting up the manager, the {@link
     * TieredStorageMemorySpec}s for different tiered storages should be ready to indicate each
     * tiered storage's memory requirement specs.
     *
     * @param bufferPool the local buffer pool
     * @param storageMemorySpecs the memory specs for different tiered storages
     */
    void setup(BufferPool bufferPool, List<TieredStorageMemorySpec> storageMemorySpecs);

    /**
     * Set the {@link TaskIOMetricGroup} for this memory manager.
     *
     * @param metricGroup the metric group to set
     */
    void setMetricGroup(TaskIOMetricGroup metricGroup);

    /**
     * Register a listener to listen the buffer reclaim request from the {@link
     * TieredStorageMemoryManager}.
     *
     * <p>When the left buffers in the {@link BufferPool} are not enough, {@link
     * TieredStorageMemoryManager} will try to reclaim the buffers from the memory owners.
     *
     * @param onBufferReclaimRequest a {@link Runnable} to process the buffer reclaim request
     */
    void listenBufferReclaimRequest(Runnable onBufferReclaimRequest);

    /**
     * Request a {@link BufferBuilder} instance from {@link BufferPool} for a specific owner. The
     * {@link TieredStorageMemoryManagerImpl} will not check whether a buffer can be requested. The
     * manager only records the number of requested buffers. If the buffers in the {@link
     * BufferPool} is not enough, the manager will request each tiered storage to reclaim their
     * requested buffers as much as possible.
     *
     * <p>This is not thread safe and is expected to be called only from the task thread.
     *
     * @param owner the owner to request buffer
     * @return the requested buffer
     */
    BufferBuilder requestBufferBlocking(Object owner);

    /**
     * Return the number of the non-reclaimable buffers for the owner.
     *
     * <p>Note that the available buffers are calculated dynamically based on some conditions, for
     * example, the state of the {@link BufferPool}, the {@link TieredStorageMemorySpec} of the
     * owner, etc. So the caller should always check before requesting non-reclaimable buffers.
     *
     * <p>When invoking this method, the caller should be aware that the return value may
     * occasionally be negative. This is due to the possibility of the buffer pool size shrinking to
     * a point where it is smaller than the buffers owned by other users. In such cases, the maximum
     * non-reclaimable buffer value returned may be negative.
     */
    int getMaxNonReclaimableBuffers(Object owner);

    /**
     * Return the number of requested buffers belonging to a specific owner.
     *
     * @param owner the owner of requesting buffers
     * @return the number of requested buffers belonging to the owner.
     */
    int numOwnerRequestedBuffer(Object owner);

    /**
     * Notify the memory manager that transferring one buffer's ownership from the old owner to the
     * new owner.
     *
     * @param oldOwner the old owner of one buffer
     * @param newOwner the new owner of one buffer
     * @param buffer the buffer to transfer the ownership
     */
    void transferBufferOwnership(Object oldOwner, Object newOwner, Buffer buffer);

    /**
     * Release all the resources(if exists) and check the state of the {@link
     * TieredStorageMemoryManager}.
     */
    void release();
}
