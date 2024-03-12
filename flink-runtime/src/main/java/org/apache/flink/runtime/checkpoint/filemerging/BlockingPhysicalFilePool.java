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

package org.apache.flink.runtime.checkpoint.filemerging;

import org.apache.flink.runtime.state.CheckpointedStateScope;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Blocking {@link PhysicalFilePool} which may block when polling physical files. This class try
 * best to reuse a physical file until its size > maxFileSize.
 */
public class BlockingPhysicalFilePool extends PhysicalFilePool {

    private final AtomicBoolean exclusivePhysicalFilePoolInitialized;

    public BlockingPhysicalFilePool(
            long maxFileSize, PhysicalFile.PhysicalFileCreator physicalFileCreator) {
        super(maxFileSize, physicalFileCreator);
        this.exclusivePhysicalFilePoolInitialized = new AtomicBoolean(false);
    }

    @Override
    public boolean tryPutFile(
            FileMergingSnapshotManager.SubtaskKey subtaskKey, PhysicalFile physicalFile)
            throws IOException {
        if (physicalFile.getSize() < maxFileSize) {
            return getFileQueue(subtaskKey, physicalFile.getScope()).offer(physicalFile);
        } else {
            getFileQueue(subtaskKey, physicalFile.getScope())
                    .offer(physicalFileCreator.perform(subtaskKey, physicalFile.getScope()));
            return false;
        }
    }

    @Override
    @Nonnull
    public PhysicalFile pollFile(
            FileMergingSnapshotManager.SubtaskKey subtaskKey, CheckpointedStateScope scope)
            throws IOException {
        initialize(subtaskKey, scope);
        try {
            return ((BlockingQueue<PhysicalFile>) getFileQueue(subtaskKey, scope)).take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void initialize(
            FileMergingSnapshotManager.SubtaskKey subtaskKey, CheckpointedStateScope scope)
            throws IOException {
        if (scope.equals(CheckpointedStateScope.SHARED)) {
            AtomicBoolean init = new AtomicBoolean(false);
            Queue<PhysicalFile> fileQueue =
                    sharedPhysicalFilePoolBySubtask.computeIfAbsent(
                            subtaskKey,
                            key -> {
                                init.set(true);
                                return createFileQueue();
                            });
            if (init.get()) {
                fileQueue.offer(physicalFileCreator.perform(subtaskKey, scope));
            }
        } else if (scope.equals(CheckpointedStateScope.EXCLUSIVE)
                && exclusivePhysicalFilePoolInitialized.compareAndSet(false, true)) {
            getFileQueue(subtaskKey, scope).offer(physicalFileCreator.perform(subtaskKey, scope));
        }
    }

    @Override
    protected Queue<PhysicalFile> createFileQueue() {
        return new LinkedBlockingQueue<>();
    }
}
