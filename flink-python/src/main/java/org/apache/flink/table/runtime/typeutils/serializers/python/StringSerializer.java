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

package org.apache.flink.table.runtime.typeutils.serializers.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.runtime.util.StringUtf8Utils;

import java.io.IOException;

/**
 * We create the StringSerializer instead of using the StringSerializer of flink-core module because
 * the StringSerializer of flink-core module serialize every Char of String in serialize method and
 * deserialize the Char to build the String. We want to convert String to UTF-8 bytes to serialize
 * which is compatible with BinaryStringSerializer.
 *
 * <p>So we create this StringSerializer (only used in Java and Python data communication in udf).
 *
 * <p>StringSerializer for String.
 */
@Internal
public class StringSerializer extends TypeSerializerSingleton<String> {

    private static final long serialVersionUID = 1L;

    public static final StringSerializer INSTANCE = new StringSerializer();

    private static final String EMPTY = "";

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public String createInstance() {
        return EMPTY;
    }

    @Override
    public String copy(String from) {
        return from;
    }

    @Override
    public String copy(String from, String reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(String record, DataOutputView target) throws IOException {
        if (record == null) {
            throw new IllegalArgumentException("The String record must not be null.");
        }
        byte[] bytes = StringUtf8Utils.encodeUTF8(record);
        target.writeInt(bytes.length);
        target.write(bytes);
    }

    @Override
    public String deserialize(DataInputView source) throws IOException {
        int len = source.readInt();
        byte[] bytes = new byte[len];
        source.read(bytes);
        return StringUtf8Utils.decodeUTF8(bytes, 0, len);
    }

    @Override
    public String deserialize(String reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int len = source.readInt();
        target.writeInt(len);
        byte[] bytes = new byte[len];
        source.read(bytes);
        target.write(bytes);
    }

    @Override
    public TypeSerializerSnapshot<String> snapshotConfiguration() {
        return new StringSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class StringSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<String> {

        public StringSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
