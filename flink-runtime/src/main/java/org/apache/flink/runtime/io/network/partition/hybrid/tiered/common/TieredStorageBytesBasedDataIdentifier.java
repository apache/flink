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

import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;

/** The bytes based unique identification for the Tiered Storage. */
public abstract class TieredStorageBytesBasedDataIdentifier
        implements TieredStorageDataIdentifier, Serializable {

    private static final long serialVersionUID = 1L;

    /** The bytes data of this identifier. */
    protected final byte[] bytes;

    protected final int hashCode;

    public TieredStorageBytesBasedDataIdentifier(byte[] bytes) {
        checkArgument(bytes != null, "Must be not null.");

        this.bytes = bytes;
        this.hashCode = Arrays.hashCode(bytes);
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }

        if (that == null || getClass() != that.getClass()) {
            return false;
        }

        TieredStorageBytesBasedDataIdentifier thatID = (TieredStorageBytesBasedDataIdentifier) that;
        return hashCode == thatID.hashCode && Arrays.equals(bytes, thatID.bytes);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return "TieredStorageBytesBasedDataIdentifier{"
                + "ID="
                + StringUtils.byteToHexString(bytes)
                + '}';
    }
}
