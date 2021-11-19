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

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.junit.Test;

import java.io.IOException;

import static org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus.ACTIVE_STATUS;
import static org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus.IDLE_STATUS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link StreamElementSerializer}. */
public class StreamElementSerializerTest {

    @Test
    public void testDeepDuplication() {
        @SuppressWarnings("unchecked")
        TypeSerializer<Long> serializer1 = (TypeSerializer<Long>) mock(TypeSerializer.class);

        @SuppressWarnings("unchecked")
        TypeSerializer<Long> serializer2 = (TypeSerializer<Long>) mock(TypeSerializer.class);

        when(serializer1.duplicate()).thenReturn(serializer2);

        StreamElementSerializer<Long> streamRecSer = new StreamElementSerializer<>(serializer1);

        assertEquals(serializer1, streamRecSer.getContainedTypeSerializer());

        StreamElementSerializer<Long> copy = streamRecSer.duplicate();
        assertNotEquals(copy, streamRecSer);
        assertNotEquals(
                copy.getContainedTypeSerializer(), streamRecSer.getContainedTypeSerializer());
    }

    @Test
    public void testBasicProperties() {
        StreamElementSerializer<Long> streamRecSer =
                new StreamElementSerializer<>(LongSerializer.INSTANCE);

        assertFalse(streamRecSer.isImmutableType());
        assertEquals(Long.class, streamRecSer.createInstance().getValue().getClass());
        assertEquals(-1L, streamRecSer.getLength());
    }

    @Test
    public void testCopy() throws IOException {
        final StreamElementSerializer<String> serializer =
                new StreamElementSerializer<>(StringSerializer.INSTANCE);

        StreamRecord<String> withoutTimestamp = new StreamRecord<>("test 1 2 分享基督耶穌的愛給們，開拓雙贏!");
        assertEquals(withoutTimestamp, serializer.copy(withoutTimestamp));

        StreamRecord<String> withTimestamp = new StreamRecord<>("one more test 拓 們 分", 77L);
        assertEquals(withTimestamp, serializer.copy(withTimestamp));

        StreamRecord<String> negativeTimestamp = new StreamRecord<>("他", Long.MIN_VALUE);
        assertEquals(negativeTimestamp, serializer.copy(negativeTimestamp));

        Watermark positiveWatermark = new Watermark(13);
        assertEquals(positiveWatermark, serializer.copy(positiveWatermark));

        Watermark negativeWatermark = new Watermark(-4647654567676555876L);
        assertEquals(negativeWatermark, serializer.copy(negativeWatermark));

        final WatermarkStatus idle = new WatermarkStatus(IDLE_STATUS);
        assertEquals(idle, serializer.copy(idle));

        final WatermarkStatus active = new WatermarkStatus(ACTIVE_STATUS);
        assertEquals(active, serializer.copy(active));

        LatencyMarker latencyMarker =
                new LatencyMarker(System.currentTimeMillis(), new OperatorID(-1, 1), 2);
        assertEquals(latencyMarker, serializer.copy(latencyMarker));
    }

    @Test
    public void testCopyWithReuse() {
        final StreamElementSerializer<String> serializer =
                new StreamElementSerializer<>(StringSerializer.INSTANCE);
        final StreamRecord<String> reuse = new StreamRecord<>("");

        StreamRecord<String> withoutTimestamp = new StreamRecord<>("test 1 2 分享基督耶穌的愛給們，開拓雙贏!");
        assertEquals(withoutTimestamp, serializer.copy(withoutTimestamp, reuse));

        StreamRecord<String> withTimestamp = new StreamRecord<>("one more test 拓 們 分", 77L);
        assertEquals(withTimestamp, serializer.copy(withTimestamp, reuse));

        StreamRecord<String> negativeTimestamp = new StreamRecord<>("他", Long.MIN_VALUE);
        assertEquals(negativeTimestamp, serializer.copy(negativeTimestamp, reuse));

        Watermark positiveWatermark = new Watermark(13);
        assertEquals(positiveWatermark, serializer.copy(positiveWatermark, reuse));

        Watermark negativeWatermark = new Watermark(-4647654567676555876L);
        assertEquals(negativeWatermark, serializer.copy(negativeWatermark, reuse));

        final WatermarkStatus idle = new WatermarkStatus(IDLE_STATUS);
        assertEquals(idle, serializer.copy(idle, reuse));

        final WatermarkStatus active = new WatermarkStatus(ACTIVE_STATUS);
        assertEquals(active, serializer.copy(active, reuse));

        LatencyMarker latencyMarker =
                new LatencyMarker(System.currentTimeMillis(), new OperatorID(-1, 1), 2);
        assertEquals(latencyMarker, serializer.copy(latencyMarker, reuse));
    }

    @Test
    public void testSerialization() throws Exception {
        final StreamElementSerializer<String> serializer =
                new StreamElementSerializer<>(StringSerializer.INSTANCE);

        StreamRecord<String> withoutTimestamp = new StreamRecord<>("test 1 2 分享基督耶穌的愛給們，開拓雙贏!");
        assertEquals(withoutTimestamp, serializeAndDeserialize(withoutTimestamp, serializer));

        StreamRecord<String> withTimestamp = new StreamRecord<>("one more test 拓 們 分", 77L);
        assertEquals(withTimestamp, serializeAndDeserialize(withTimestamp, serializer));

        StreamRecord<String> negativeTimestamp = new StreamRecord<>("他", Long.MIN_VALUE);
        assertEquals(negativeTimestamp, serializeAndDeserialize(negativeTimestamp, serializer));

        Watermark positiveWatermark = new Watermark(13);
        assertEquals(positiveWatermark, serializeAndDeserialize(positiveWatermark, serializer));

        Watermark negativeWatermark = new Watermark(-4647654567676555876L);
        assertEquals(negativeWatermark, serializeAndDeserialize(negativeWatermark, serializer));

        final WatermarkStatus idle = new WatermarkStatus(IDLE_STATUS);
        assertEquals(idle, serializeAndDeserialize(idle, serializer));

        final WatermarkStatus active = new WatermarkStatus(ACTIVE_STATUS);
        assertEquals(active, serializeAndDeserialize(active, serializer));

        LatencyMarker latencyMarker =
                new LatencyMarker(System.currentTimeMillis(), new OperatorID(-1, -1), 1);
        assertEquals(latencyMarker, serializeAndDeserialize(latencyMarker, serializer));
    }

    @Test
    public void testSerializationWithReuse() throws Exception {
        final StreamElementSerializer<String> serializer =
                new StreamElementSerializer<>(StringSerializer.INSTANCE);
        final StreamRecord<String> reuse = new StreamRecord<>("");

        StreamRecord<String> withoutTimestamp = new StreamRecord<>("test 1 2 分享基督耶穌的愛給們，開拓雙贏!");
        assertEquals(
                withoutTimestamp,
                serializeAndDeserializeWithReuse(withoutTimestamp, serializer, reuse));

        StreamRecord<String> withTimestamp = new StreamRecord<>("one more test 拓 們 分", 77L);
        assertEquals(
                withTimestamp, serializeAndDeserializeWithReuse(withTimestamp, serializer, reuse));

        StreamRecord<String> negativeTimestamp = new StreamRecord<>("他", Long.MIN_VALUE);
        assertEquals(
                negativeTimestamp,
                serializeAndDeserializeWithReuse(negativeTimestamp, serializer, reuse));

        Watermark positiveWatermark = new Watermark(13);
        assertEquals(
                positiveWatermark,
                serializeAndDeserializeWithReuse(positiveWatermark, serializer, reuse));

        Watermark negativeWatermark = new Watermark(-4647654567676555876L);
        assertEquals(
                negativeWatermark,
                serializeAndDeserializeWithReuse(negativeWatermark, serializer, reuse));

        final WatermarkStatus idle = new WatermarkStatus(IDLE_STATUS);
        assertEquals(idle, serializeAndDeserializeWithReuse(idle, serializer, reuse));

        final WatermarkStatus active = new WatermarkStatus(ACTIVE_STATUS);
        assertEquals(active, serializeAndDeserializeWithReuse(active, serializer, reuse));

        LatencyMarker latencyMarker =
                new LatencyMarker(System.currentTimeMillis(), new OperatorID(-1, -1), 1);
        assertEquals(
                latencyMarker, serializeAndDeserializeWithReuse(latencyMarker, serializer, reuse));
    }

    @SuppressWarnings("unchecked")
    private static <T, X extends StreamElement> X serializeAndDeserialize(
            X record, StreamElementSerializer<T> serializer) throws IOException {

        DataOutputSerializer output = new DataOutputSerializer(32);
        serializer.serialize(record, output);

        // additional binary copy step
        DataInputDeserializer copyInput =
                new DataInputDeserializer(output.getSharedBuffer(), 0, output.length());
        DataOutputSerializer copyOutput = new DataOutputSerializer(32);
        serializer.copy(copyInput, copyOutput);

        DataInputDeserializer input =
                new DataInputDeserializer(copyOutput.getSharedBuffer(), 0, copyOutput.length());
        return (X) serializer.deserialize(input);
    }

    @SuppressWarnings("unchecked")
    private static <T, X extends StreamElement> X serializeAndDeserializeWithReuse(
            X record, StreamElementSerializer<T> serializer, StreamElement reuse)
            throws IOException {

        DataOutputSerializer output = new DataOutputSerializer(32);
        serializer.serialize(record, output);

        // additional binary copy step
        DataInputDeserializer copyInput =
                new DataInputDeserializer(output.getSharedBuffer(), 0, output.length());
        DataOutputSerializer copyOutput = new DataOutputSerializer(32);
        serializer.copy(copyInput, copyOutput);

        DataInputDeserializer input =
                new DataInputDeserializer(copyOutput.getSharedBuffer(), 0, copyOutput.length());
        return (X) serializer.deserialize(reuse, input);
    }
}
