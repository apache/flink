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
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.VarCharType;

/** An internal data structure representing data of {@link CharType} and {@link VarCharType}. */
@PublicEvolving
public interface StringData extends Comparable<StringData> {

    /**
     * Converts this {@link StringData} object to a UTF-8 byte array.
     *
     * <p>Note: The returned byte array may be reused.
     */
    byte[] toBytes();

    /** Converts this {@link StringData} object to a {@link String}. */
    String toString();

    // ------------------------------------------------------------------------------------------
    // Construction Utilities
    // ------------------------------------------------------------------------------------------

    /** Creates an instance of {@link StringData} from the given {@link String}. */
    static StringData fromString(String str) {
        return BinaryStringData.fromString(str);
    }

    /**
     * Creates an instance of {@link StringData} by wrapping the given UTF-8 byte array in O(1)
     * without copying or validating it. The caller is responsible for ensuring the bytes are
     * well-formed UTF-8; use {@link #fromUtf8Bytes(byte[])} if validation is required.
     */
    static StringData fromBytes(byte[] bytes) {
        return BinaryStringData.fromBytes(bytes);
    }

    /**
     * Creates an instance of {@link StringData} by wrapping the given UTF-8 byte range in O(1)
     * without copying or validating it. The caller is responsible for ensuring the bytes are
     * well-formed UTF-8; use {@link #fromUtf8Bytes(byte[], int, int)} if validation is required.
     */
    static StringData fromBytes(byte[] bytes, int offset, int numBytes) {
        return BinaryStringData.fromBytes(bytes, offset, numBytes);
    }

    /**
     * Creates an instance of {@link StringData} from the given UTF-8 byte array, walking the input
     * once in O(n) to verify it is well-formed UTF-8. Returns {@code null} if the input is {@code
     * null}. Throws {@link org.apache.flink.table.api.TableRuntimeException} on invalid UTF-8.
     *
     * <p>Connector authors should prefer this method over {@link #fromBytes(byte[])} when ingesting
     * data from external systems whose UTF-8 conformance is not guaranteed: the strict variant
     * surfaces the error at the source rather than letting silent {@code U+FFFD} substitution
     * propagate downstream. Use {@link #fromBytes(byte[])} when the bytes are already known to be
     * valid and the O(n) check can be skipped.
     */
    static StringData fromUtf8Bytes(byte[] bytes) {
        return BinaryStringData.fromUtf8Bytes(bytes);
    }

    /**
     * Creates an instance of {@link StringData} from the given UTF-8 byte range, walking it once in
     * O(n) to verify it is well-formed UTF-8. Returns {@code null} if the input is {@code null}.
     * Throws {@link org.apache.flink.table.api.TableRuntimeException} on invalid UTF-8.
     *
     * @see #fromUtf8Bytes(byte[])
     */
    static StringData fromUtf8Bytes(byte[] bytes, int offset, int numBytes) {
        return BinaryStringData.fromUtf8Bytes(bytes, offset, numBytes);
    }
}
