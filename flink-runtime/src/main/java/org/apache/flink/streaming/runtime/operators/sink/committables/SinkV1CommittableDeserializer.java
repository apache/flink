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

package org.apache.flink.streaming.runtime.operators.sink.committables;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputView;

import java.io.IOException;
import java.util.List;

/**
 * This class offers the possibility to deserialize committables that have been written with older
 * Flink releases (i.e. 1.13, 1.14).
 */
@Internal
public class SinkV1CommittableDeserializer {
    /**
     * It is important to keep this number consistent with the number used by the {@code
     * StreamingCommitterStateSerializer} in Flink 1.13 and 1.14.
     */
    @VisibleForTesting public static final int MAGIC_NUMBER = 0xb91f252c;

    public static <T> List<T> readVersionAndDeserializeList(
            SimpleVersionedSerializer<T> serializer, DataInputView in) throws IOException {
        validateMagicNumber(in);
        return SimpleVersionedSerialization.readVersionAndDeserializeList(serializer, in);
    }

    private static void validateMagicNumber(DataInputView in) throws IOException {
        final int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IllegalStateException(
                    String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
        }
    }
}
