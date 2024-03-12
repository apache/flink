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
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A Non-Blocking {@link PhysicalFilePool} which will always provide usable physical file without
 * blocking. It may create many physical files if {@link
 * NonBlockingPhysicalFilePool#pollFile(FileMergingSnapshotManager.SubtaskKey,
 * CheckpointedStateScope)} frequently.
 */
public class NonBlockingPhysicalFilePool extends PhysicalFilePool {

    public NonBlockingPhysicalFilePool(
            long maxFileSize, PhysicalFile.PhysicalFileCreator physicalFileCreator) {
        super(maxFileSize, physicalFileCreator);
    }

    @Override
    public boolean tryPutFile(
            FileMergingSnapshotManager.SubtaskKey subtaskKey, PhysicalFile physicalFile) {
        return physicalFile.getSize() < maxFileSize
                && getFileQueue(subtaskKey, physicalFile.getScope()).offer(physicalFile);
    }

    @Override
    @Nonnull
    public PhysicalFile pollFile(
            FileMergingSnapshotManager.SubtaskKey subtaskKey, CheckpointedStateScope scope)
            throws IOException {
        PhysicalFile physicalFile = getFileQueue(subtaskKey, scope).poll();
        return physicalFile == null ? physicalFileCreator.perform(subtaskKey, scope) : physicalFile;
    }

    @Override
    protected Queue<PhysicalFile> createFileQueue() {
        return new ConcurrentLinkedQueue<>();
    }
}
