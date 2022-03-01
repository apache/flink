/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.utils;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Random;

/** Test class for {@link ByteArrayWrapperSerializer}. */
public class ByteArrayWrapperSerializerTest extends SerializerTestBase<ByteArrayWrapper> {

    private final Random rnd = new Random(346283764872L);

    private static final ByteArrayWrapper EMPTY_ARRAY = new ByteArrayWrapper(new byte[] {});

    @Override
    protected TypeSerializer<ByteArrayWrapper> createSerializer() {
        return new ByteArrayWrapperSerializer();
    }

    @Override
    protected Class<ByteArrayWrapper> getTypeClass() {
        return ByteArrayWrapper.class;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected ByteArrayWrapper[] getTestData() {
        return new ByteArrayWrapper[] {
            EMPTY_ARRAY,
            randomByteArray(rnd.nextInt(1024 * 1024)),
            randomByteArray(1),
            randomByteArray(2),
            randomByteArray(1024 * 1024),
            randomByteArray(32 * 1024 * 1024),
        };
    }

    private ByteArrayWrapper randomByteArray(int len) {
        byte[] data = new byte[len];
        rnd.nextBytes(data);
        return new ByteArrayWrapper(data);
    }
}
