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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

class SinkTestUtil {
    static StreamRecord<byte[]> committableRecord(String element) {
        return new StreamRecord<>(toBytes(element));
    }

    static List<StreamRecord<byte[]>> committableRecords(Collection<String> elements) {
        return elements.stream().map(SinkTestUtil::committableRecord).collect(Collectors.toList());
    }

    static List<byte[]> toBytes(String... elements) {
        return Arrays.stream(elements).map(SinkTestUtil::toBytes).collect(Collectors.toList());
    }

    static List<byte[]> toBytes(Collection<String> elements) {
        return elements.stream().map(SinkTestUtil::toBytes).collect(Collectors.toList());
    }

    static byte[] toBytes(String obj) {
        try {
            return SimpleVersionedSerialization.writeVersionAndSerialize(
                    TestSink.StringCommittableSerializer.INSTANCE, obj);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static List<String> fromRecords(Collection<StreamRecord<byte[]>> elements) {
        return elements.stream().map(SinkTestUtil::fromRecord).collect(Collectors.toList());
    }

    static List<StreamElement> fromOutput(Collection<Object> elements) {
        return elements.stream()
                .map(
                        element -> {
                            if (element instanceof StreamRecord) {
                                return new StreamRecord<>(
                                        fromRecord((StreamRecord<byte[]>) element));
                            }
                            return (StreamElement) element;
                        })
                .collect(Collectors.toList());
    }

    static String fromRecord(StreamRecord<byte[]> obj) {
        return fromBytes(obj.getValue());
    }

    static String fromBytes(byte[] obj) {
        try {
            return SimpleVersionedSerialization.readVersionAndDeSerialize(
                    TestSink.StringCommittableSerializer.INSTANCE, obj);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
