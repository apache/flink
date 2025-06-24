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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.common;

import java.io.Serializable;
import java.util.Objects;

/** Identifier of an InputChannel. */
public class TieredStorageInputChannelId implements TieredStorageDataIdentifier, Serializable {

    private static final long serialVersionUID = 1L;

    private final int inputChannelId;

    public TieredStorageInputChannelId(int inputChannelId) {
        this.inputChannelId = inputChannelId;
    }

    public int getInputChannelId() {
        return inputChannelId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(inputChannelId);
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }

        if (that == null || getClass() != that.getClass()) {
            return false;
        }

        TieredStorageInputChannelId thatID = (TieredStorageInputChannelId) that;
        return inputChannelId == thatID.inputChannelId;
    }

    @Override
    public String toString() {
        return "TieredStorageInputChannelId{" + "Id=" + inputChannelId + '}';
    }
}
