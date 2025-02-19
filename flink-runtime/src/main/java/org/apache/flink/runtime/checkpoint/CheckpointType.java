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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.Internal;

import java.util.Objects;

/** The type of checkpoint to perform. */
@Internal
public final class CheckpointType implements SnapshotType {
    private volatile boolean typeResolved = false;
    private volatile boolean isFull = false;

    /** A checkpoint, full or incremental. */
    public static final CheckpointType CHECKPOINT =
            new CheckpointType("Checkpoint", SharingFilesStrategy.FORWARD_BACKWARD);

    /** A checkpoint that is explicitly marked as full */
    public static final CheckpointType FULL_CHECKPOINT =
            new CheckpointType("Full Checkpoint", SharingFilesStrategy.FORWARD, true);

    private final String name;
    private final SharingFilesStrategy sharingFilesStrategy;

    private CheckpointType(String name, SharingFilesStrategy sharingFilesStrategy) {
        this(name, sharingFilesStrategy, false);
    }

    private CheckpointType(
            String name, SharingFilesStrategy sharingFilesStrategy, boolean preResolvedAsFull) {
        this.name = name;
        this.sharingFilesStrategy = sharingFilesStrategy;
        if (preResolvedAsFull) {
            this.isFull = true;
            this.typeResolved = true;
        }
    }

    public boolean isSavepoint() {
        return false;
    }

    public String getName() {
        return name;
    }

    public SharingFilesStrategy getSharingFilesStrategy() {
        return sharingFilesStrategy;
    }

    public boolean isFull() {
        return typeResolved && isFull;
    }

    public void resolveType(boolean isFull) {
        if (!typeResolved) {
            this.isFull = isFull;
            this.typeResolved = true;
        }
    }

    public boolean isTypeResolved() {
        return typeResolved;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CheckpointType type = (CheckpointType) o;
        return typeResolved == type.typeResolved
                && isFull == type.isFull
                && name.equals(type.name)
                && sharingFilesStrategy == type.sharingFilesStrategy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, sharingFilesStrategy, typeResolved, isFull);
    }

    @Override
    public String toString() {
        return "CheckpointType{"
                + "name='"
                + name
                + '\''
                + ", sharingFilesStrategy="
                + sharingFilesStrategy
                + ", typeResolved="
                + typeResolved
                + ", isFull="
                + isFull
                + '}';
    }
}
