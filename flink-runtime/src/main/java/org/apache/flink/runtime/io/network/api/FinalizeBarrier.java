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

package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;
import java.util.Objects;

/**
 * A prioritized event inserted when received EndOfPartition to notify the checkpoint barrier. It
 * could be viewed as a notification that all the barriers whose id is greater than or equal to
 * {@code nextBarrierId} will need inserting.
 */
public class FinalizeBarrier extends RuntimeEvent {
    private final long nextBarrierId;

    public FinalizeBarrier(long nextBarrierId) {
        this.nextBarrierId = nextBarrierId;
    }

    public long getNextBarrierId() {
        return nextBarrierId;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    @Override
    public void read(DataInputView in) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FinalizeBarrier that = (FinalizeBarrier) o;
        return nextBarrierId == that.nextBarrierId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nextBarrierId);
    }

    @Override
    public String toString() {
        return "FinalizeBarrier{" + "nextBarrierId=" + nextBarrierId + '}';
    }
}
