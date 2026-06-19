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

package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

/** A self-contained serialized value to decouple from user values and transfer on wire. */
final class RpcSerializedValue implements Serializable {
    private static final long serialVersionUID = -4388571068440835689L;

    @Nullable private final byte[] serializedData;

    private RpcSerializedValue(@Nullable byte[] serializedData) {
        this.serializedData = serializedData;
    }

    @Nullable
    public byte[] getSerializedData() {
        return serializedData;
    }

    /** Return length of serialized data, zero if no serialized data. */
    public int getSerializedDataLength() {
        return serializedData == null ? 0 : serializedData.length;
    }

    @Nullable
    public <T> T deserializeValue(ClassLoader loader) throws IOException, ClassNotFoundException {
        Preconditions.checkNotNull(loader, "No classloader has been passed");
        return serializedData == null
                ? null
                : InstantiationUtil.deserializeObject(serializedData, loader);
    }

    /**
     * Construct a serialized value to transfer on wire.
     *
     * @param value nullable value
     * @return serialized value to transfer on wire
     * @throws IOException exception during value serialization
     */
    public static RpcSerializedValue valueOf(@Nullable Object value) throws IOException {
        byte[] serializedData = value == null ? null : InstantiationUtil.serializeObject(value);
        return new RpcSerializedValue(serializedData);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof RpcSerializedValue) {
            RpcSerializedValue other = (RpcSerializedValue) o;
            return Arrays.equals(serializedData, other.serializedData);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(serializedData);
    }

    @Override
    public String toString() {
        return serializedData == null ? "RpcSerializedValue(null)" : "RpcSerializedValue";
    }
}
