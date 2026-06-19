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

/** Stores first and last SQN for a record. */
class MetaSqnInfo {
    public final long highSqn;
    public final long size;

    public MetaSqnInfo(long highSqn, long size) {
        Preconditions.checkArgument(size >= 0);
        this.highSqn = highSqn;
        this.size = size;
    }

    public static MetaSqnInfo of(long first, long last) {
        return new MetaSqnInfo(first, last);
    }

    @Override
    public String toString() {
        return "MetaSqnInfo{" + "firstSqn=" + highSqn + ", lastSqn=" + size + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MetaSqnInfo)) {
            return false;
        }
        MetaSqnInfo that = (MetaSqnInfo) o;
        return highSqn == that.highSqn && size == that.size;
    }

    @Override
    public int hashCode() {
        return Objects.hash(highSqn, size);
    }
}
