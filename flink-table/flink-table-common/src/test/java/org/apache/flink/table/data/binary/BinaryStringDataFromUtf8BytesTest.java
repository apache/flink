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

package org.apache.flink.table.data.binary;

import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.StringData;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link BinaryStringData#fromUtf8Bytes(byte[])} and {@link StringData#fromUtf8Bytes}.
 * Validator coverage lives in {@link StringUtf8UtilsTest}; this file just covers the wrapping
 * concerns: null tolerance, exception type, and the error message contract.
 */
class BinaryStringDataFromUtf8BytesTest {

    @Test
    void testFromUtf8Bytes() {
        final byte[] hello = "Hello".getBytes(StandardCharsets.UTF_8);

        // Valid input wraps the bytes.
        assertThat(BinaryStringData.fromUtf8Bytes(hello))
                .isEqualTo(BinaryStringData.fromBytes(hello));

        // null in -> null out, mirroring fromString.
        assertThat(BinaryStringData.fromUtf8Bytes((byte[]) null)).isNull();
        assertThat(StringData.fromUtf8Bytes((byte[]) null)).isNull();

        // Invalid UTF-8 throws TableRuntimeException with the relative byte index.
        assertThatThrownBy(() -> BinaryStringData.fromUtf8Bytes(new byte[] {'A', 'B', (byte) 0x80}))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("Invalid UTF-8 byte at index 2 of 3");
    }
}
