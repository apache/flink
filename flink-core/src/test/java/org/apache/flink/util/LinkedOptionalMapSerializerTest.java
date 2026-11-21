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

package org.apache.flink.util;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.LinkedOptionalMapSerializer.readOptionalMap;
import static org.apache.flink.util.LinkedOptionalMapSerializer.writeOptionalMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LinkedOptionalMapSerializer}. */
class LinkedOptionalMapSerializerTest {

    @Test
    void roundTripPreservesOrderAndAbsentEntries() throws IOException {
        LinkedOptionalMap<String, Integer> map = new LinkedOptionalMap<>();
        map.put("c", "key-c", 3);
        map.put("a", null, 1);
        map.put("b", "key-b", null);
        map.put("d", null, null);

        LinkedOptionalMap<String, Integer> restored =
                readOptionalMap(
                        serialize(map),
                        (in, keyName) -> in.readUTF(),
                        (in, keyName) -> in.readInt());

        assertThat(entries(restored))
                .containsExactly("c=key-c/3", "a=null/1", "b=key-b/null", "d=null/null");
        assertThat(restored.absentKeysOrValues()).containsExactly("a", "b", "d");
    }

    @Test
    void roundTripOfEmptyMap() throws IOException {
        LinkedOptionalMap<String, Integer> restored =
                readOptionalMap(
                        serialize(new LinkedOptionalMap<>()),
                        (in, keyName) -> in.readUTF(),
                        (in, keyName) -> in.readInt());

        assertThat(restored.size()).isZero();
    }

    @Test
    void readerThatConsumesOnlyPartOfAFrameDoesNotCorruptLaterEntries() throws IOException {
        LinkedOptionalMap<String, Integer> map = new LinkedOptionalMap<>();
        map.put("first", "key-1", 1);
        map.put("second", "key-2", 2);

        DataOutputSerializer out = new DataOutputSerializer(64);
        // Each key frame carries trailing bytes that the key reader below never reads.
        writeOptionalMap(
                out,
                map,
                (view, key) -> {
                    view.writeUTF(key);
                    view.writeLong(Long.MAX_VALUE);
                },
                DataOutputView::writeInt);

        LinkedOptionalMap<String, Integer> restored =
                readOptionalMap(
                        new DataInputDeserializer(out.getCopyOfBuffer()),
                        (in, keyName) -> in.readUTF(),
                        (in, keyName) -> in.readInt());

        assertThat(entries(restored)).containsExactly("first=key-1/1", "second=key-2/2");
    }

    @Test
    void readersReceiveKeyNameAndMayReturnNullToMarkEntryAbsent() throws IOException {
        LinkedOptionalMap<String, Integer> map = new LinkedOptionalMap<>();
        map.put("kept", "key-1", 1);
        map.put("dropped", "key-2", 2);

        List<String> keyReaderCalls = new ArrayList<>();
        List<String> valueReaderCalls = new ArrayList<>();
        LinkedOptionalMap<String, Integer> restored =
                readOptionalMap(
                        serialize(map),
                        (in, keyName) -> {
                            keyReaderCalls.add(keyName);
                            String key = in.readUTF();
                            return keyName.equals("dropped") ? null : key;
                        },
                        (in, keyName) -> {
                            valueReaderCalls.add(keyName);
                            return in.readInt();
                        });

        assertThat(keyReaderCalls).containsExactly("kept", "dropped");
        assertThat(valueReaderCalls).containsExactly("kept", "dropped");
        assertThat(entries(restored)).containsExactly("kept=key-1/1", "dropped=null/2");
        assertThat(restored.absentKeysOrValues()).containsExactly("dropped");
    }

    @Test
    void corruptedHeaderIsRejected() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(16);
        out.writeLong(42L);
        out.writeInt(0);

        assertThatThrownBy(
                        () ->
                                readOptionalMap(
                                        new DataInputDeserializer(out.getCopyOfBuffer()),
                                        (in, keyName) -> in.readUTF(),
                                        (in, keyName) -> in.readInt()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Corrupted stream received header 42");
    }

    private static DataInputView serialize(LinkedOptionalMap<String, Integer> map)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        writeOptionalMap(out, map, DataOutputView::writeUTF, DataOutputView::writeInt);
        return new DataInputDeserializer(out.getCopyOfBuffer());
    }

    private static List<String> entries(LinkedOptionalMap<String, Integer> map) {
        List<String> entries = new ArrayList<>();
        map.forEach((keyName, key, value) -> entries.add(keyName + "=" + key + "/" + value));
        return entries;
    }
}
