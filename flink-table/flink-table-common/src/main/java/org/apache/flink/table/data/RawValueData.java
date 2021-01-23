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

package org.apache.flink.table.data;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.binary.BinaryRawValueData;
import org.apache.flink.table.types.logical.RawType;

/**
 * An internal data structure representing data of {@link RawType}.
 *
 * <p>This data structure is immutable.
 *
 * @param <T> originating class for the raw value
 */
@PublicEvolving
public interface RawValueData<T> {

    /**
     * Converts this {@link RawValueData} into a Java object.
     *
     * <p>The given serializer is required because the "raw value" might be represented in a binary
     * format and needs to be deserialized first.
     *
     * <p>Note: The returned Java object may be reused.
     */
    T toObject(TypeSerializer<T> serializer);

    /**
     * Converts this {@link RawValueData} into a byte array.
     *
     * <p>The given serializer is required because the "raw value" might be still be a Java object
     * and needs to be serialized first.
     *
     * <p>Note: The returned byte array may be reused.
     */
    byte[] toBytes(TypeSerializer<T> serializer);

    // ------------------------------------------------------------------------------------------
    // Constructor Utilities
    // ------------------------------------------------------------------------------------------

    /** Creates an instance of {@link RawValueData} from a Java object. */
    static <T> RawValueData<T> fromObject(T javaObject) {
        return BinaryRawValueData.fromObject(javaObject);
    }

    /** Creates an instance of {@link RawValueData} from the given byte array. */
    static <T> RawValueData<T> fromBytes(byte[] bytes) {
        return BinaryRawValueData.fromBytes(bytes);
    }
}
