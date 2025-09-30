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

package org.apache.flink.table.runtime.sequencedmultisetstate.linked;

import org.apache.flink.util.Preconditions;

import java.util.Objects;

/** Stores first and las SQN for a record. */
class RowSqnInfo {
    public final long firstSqn;
    public final long lastSqn;

    public RowSqnInfo(long firstSqn, long lastSqn) {
        Preconditions.checkArgument(firstSqn <= lastSqn);
        this.firstSqn = firstSqn;
        this.lastSqn = lastSqn;
    }

    public static RowSqnInfo ofSingle(long sqn) {
        return of(sqn, sqn);
    }

    public static RowSqnInfo of(long first, long last) {
        return new RowSqnInfo(first, last);
    }

    @Override
    public String toString() {
        return "RowSqnInfo{" + "firstSqn=" + firstSqn + ", lastSqn=" + lastSqn + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RowSqnInfo)) {
            return false;
        }
        RowSqnInfo that = (RowSqnInfo) o;
        return firstSqn == that.firstSqn && lastSqn == that.lastSqn;
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstSqn, lastSqn);
    }

    public RowSqnInfo withFirstSqn(long firstSqn) {
        return RowSqnInfo.of(firstSqn, lastSqn);
    }
}
