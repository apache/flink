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

package org.apache.flink.api.common.typeutils.base.array;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Random;

/**
 * A test for the {@link
 * org.apache.flink.api.common.typeutils.base.array.LongPrimitiveArraySerializer}.
 */
public class BytePrimitiveArraySerializerTest extends SerializerTestBase<byte[]> {

    private final Random rnd = new Random(346283764872L);

    @Override
    protected TypeSerializer<byte[]> createSerializer() {
        return new BytePrimitiveArraySerializer();
    }

    @Override
    protected Class<byte[]> getTypeClass() {
        return byte[].class;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected byte[][] getTestData() {
        return new byte[][] {
            randomByteArray(),
            randomByteArray(),
            new byte[] {},
            randomByteArray(),
            randomByteArray(),
            randomByteArray(),
            new byte[] {},
            randomByteArray(),
            randomByteArray(),
            randomByteArray(),
            new byte[] {}
        };
    }

    private final byte[] randomByteArray() {
        int len = rnd.nextInt(1024 * 1024);
        byte[] data = new byte[len];
        for (int i = 0; i < len; i++) {
            data[i] = (byte) rnd.nextInt();
        }
        return data;
    }
}
