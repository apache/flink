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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantBuilder;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that {@link VariantSerializer#deserialize} reads the value/metadata arrays to completion
 * even when the source returns short reads.
 */
class VariantSerializerShortReadTest {

    private static final VariantSerializer SERIALIZER = VariantSerializer.INSTANCE;

    /** A Variant large enough that its value and metadata each span many read chunks. */
    private static Variant largeVariant() {
        VariantBuilder builder = Variant.newBuilder();
        VariantBuilder.VariantObjectBuilder object = builder.object();
        for (int i = 0; i < 2048; i++) {
            object.add("resource-key-" + i, builder.of("resource-value-payload-" + i));
        }
        return object.build();
    }

    private static byte[] serialize(Variant variant) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SERIALIZER.serialize(variant, new DataOutputViewStreamWrapper(out));
        return out.toByteArray();
    }

    @Test
    void deserializeRecoversFromShortReads() throws IOException {
        Variant original = largeVariant();
        byte[] bytes = serialize(original);

        DataInputViewStreamWrapper source =
                new DataInputViewStreamWrapper(new OneByteAtATimeInputStream(bytes));

        Variant restored = SERIALIZER.deserialize(source);

        assertThat(restored).isEqualTo(original);
    }

    @Test
    void deserializeThrowsEofOnTruncatedStream() throws IOException {
        byte[] bytes = serialize(largeVariant());
        byte[] truncated = Arrays.copyOf(bytes, bytes.length - 1);

        DataInputViewStreamWrapper source =
                new DataInputViewStreamWrapper(new ByteArrayInputStream(truncated));

        assertThatThrownBy(() -> SERIALIZER.deserialize(source)).isInstanceOf(EOFException.class);
    }

    /** Returns at most one byte per {@code read(byte[], int, int)} call. */
    private static final class OneByteAtATimeInputStream extends FilterInputStream {
        OneByteAtATimeInputStream(byte[] data) {
            super(new ByteArrayInputStream(data));
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return super.read(b, off, Math.min(len, 1));
        }
    }
}
