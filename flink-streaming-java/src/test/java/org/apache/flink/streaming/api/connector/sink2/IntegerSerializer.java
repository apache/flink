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

package org.apache.flink.streaming.api.connector.sink2;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/** Simple serializer for int values. */
public class IntegerSerializer implements SimpleVersionedSerializer<Integer> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(Integer obj) throws IOException {
        final DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(obj);
        return out.getCopyOfBuffer();
    }

    @Override
    public Integer deserialize(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        return in.readInt();
    }
}
