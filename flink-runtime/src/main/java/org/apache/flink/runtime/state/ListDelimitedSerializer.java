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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates a logic of serialization and deserialization of a list with a delimiter. Used in the
 * savepoint format.
 */
public final class ListDelimitedSerializer {

    private static final byte DELIMITER = ',';

    private final DataInputDeserializer dataInputView = new DataInputDeserializer();
    private final DataOutputSerializer dataOutputView = new DataOutputSerializer(128);

    public <T> List<T> deserializeList(byte[] valueBytes, TypeSerializer<T> elementSerializer) {
        if (valueBytes == null) {
            return null;
        }

        dataInputView.setBuffer(valueBytes);

        List<T> result = new ArrayList<>();
        T next;
        while ((next = deserializeNextElement(dataInputView, elementSerializer)) != null) {
            result.add(next);
        }
        return result;
    }

    public <T> byte[] serializeList(List<T> valueList, TypeSerializer<T> elementSerializer)
            throws IOException {

        dataOutputView.clear();
        boolean first = true;

        for (T value : valueList) {
            Preconditions.checkNotNull(value, "You cannot add null to a value list.");

            if (first) {
                first = false;
            } else {
                dataOutputView.write(DELIMITER);
            }
            elementSerializer.serialize(value, dataOutputView);
        }

        return dataOutputView.getCopyOfBuffer();
    }

    /** Deserializes a single element from a serialized list. */
    public static <T> T deserializeNextElement(
            DataInputDeserializer in, TypeSerializer<T> elementSerializer) {
        try {
            if (in.available() > 0) {
                T element = elementSerializer.deserialize(in);
                if (in.available() > 0) {
                    in.readByte();
                }
                return element;
            }
        } catch (IOException e) {
            throw new FlinkRuntimeException("Unexpected list element deserialization failure", e);
        }
        return null;
    }
}
