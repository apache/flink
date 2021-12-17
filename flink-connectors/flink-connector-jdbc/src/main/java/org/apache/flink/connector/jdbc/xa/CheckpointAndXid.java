/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import javax.annotation.concurrent.ThreadSafe;
import javax.transaction.xa.Xid;

import java.util.Objects;

/**
 * A pair of checkpoint id and {@link Xid} representing a checkpoint and an associated pending
 * (prepared) XA transaction. Thread-safe (assuming immutable {@link Xid} implementation).
 */
@ThreadSafe
@Internal
public final class CheckpointAndXid {
    final long checkpointId;
    final Xid xid;
    final int attempts;
    final boolean restored;

    public Xid getXid() {
        return xid;
    }

    private CheckpointAndXid(long checkpointId, Xid xid, int attempts, boolean restored) {
        this.checkpointId = checkpointId;
        this.xid = Preconditions.checkNotNull(xid);
        this.attempts = attempts;
        this.restored = restored;
    }

    @Override
    public String toString() {
        return String.format("checkpointId=%d, xid=%s, restored=%s", checkpointId, xid, restored);
    }

    CheckpointAndXid asRestored() {
        return restored ? this : new CheckpointAndXid(checkpointId, xid, attempts, true);
    }

    static CheckpointAndXid createRestored(long checkpointId, int attempts, Xid xid) {
        return new CheckpointAndXid(checkpointId, xid, attempts, true);
    }

    static CheckpointAndXid createNew(long checkpointId, Xid xid) {
        return new CheckpointAndXid(checkpointId, xid, 0, false);
    }

    CheckpointAndXid withAttemptsIncremented() {
        return new CheckpointAndXid(checkpointId, xid, attempts + 1, restored);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CheckpointAndXid)) {
            return false;
        }
        CheckpointAndXid that = (CheckpointAndXid) o;
        return checkpointId == that.checkpointId
                && attempts == that.attempts
                && restored == that.restored
                && Objects.equals(xid, that.xid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(checkpointId, xid, attempts, restored);
    }
}
