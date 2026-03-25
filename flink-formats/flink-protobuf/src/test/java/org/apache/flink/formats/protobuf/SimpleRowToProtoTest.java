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

package org.apache.flink.formats.protobuf;

import org.apache.flink.formats.protobuf.testproto.SimpleTestMulti;
import org.apache.flink.formats.protobuf.testproto.Status;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test conversion of flink internal primitive data to proto data. */
class SimpleRowToProtoTest {
    @Test
    void testSimple() throws Exception {
        RowData row =
                GenericRowData.of(
                        1,
                        2L,
                        false,
                        0.1f,
                        0.01,
                        StringData.fromString("hello"),
                        new byte[] {1},
                        StringData.fromString("IMAGES"),
                        StringData.fromString("FINISHED"),
                        1,
                        2);

        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, SimpleTestMulti.class);
        SimpleTestMulti simpleTestMulti = SimpleTestMulti.parseFrom(bytes);
        assertThat(simpleTestMulti.hasA()).isTrue();
        assertThat(simpleTestMulti.getA()).isEqualTo(1);
        assertThat(simpleTestMulti.getB()).isEqualTo(2L);
        assertThat(simpleTestMulti.getC()).isFalse();
        assertThat(simpleTestMulti.getD()).isEqualTo(0.1f);
        assertThat(simpleTestMulti.getE()).isEqualTo(0.01d);
        assertThat(simpleTestMulti.getF()).isEqualTo("hello");
        assertThat(simpleTestMulti.getG().byteAt(0)).isEqualTo((byte) 1);
        assertThat(simpleTestMulti.getH()).isEqualTo(SimpleTestMulti.Corpus.IMAGES);
        assertThat(simpleTestMulti.getI()).isEqualTo(Status.FINISHED);
        assertThat(simpleTestMulti.getFAbc7D()).isEqualTo(1);
    }

    @Test
    void testNull() throws Exception {
        RowData row =
                GenericRowData.of(
                        null,
                        2L,
                        false,
                        0.1f,
                        0.01,
                        StringData.fromString("hello"),
                        null,
                        null,
                        null,
                        1,
                        2);

        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, SimpleTestMulti.class);
        SimpleTestMulti simpleTestMulti = SimpleTestMulti.parseFrom(bytes);
        assertThat(simpleTestMulti.hasA()).isFalse();
        assertThat(simpleTestMulti.hasG()).isFalse();
        assertThat(simpleTestMulti.hasH()).isFalse();
        assertThat(simpleTestMulti.hasI()).isFalse();
    }

    @Test
    void testEnumAsInt() throws Exception {
        RowData row =
                GenericRowData.of(
                        null, null, null, null, null, null, null, 2, // CORPUS: IMAGE
                        1, // STATUS: STARTED
                        null, null);

        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, SimpleTestMulti.class, true);
        SimpleTestMulti simpleTestMulti = SimpleTestMulti.parseFrom(bytes);
        assertThat(simpleTestMulti.getH()).isEqualTo(SimpleTestMulti.Corpus.IMAGES);
        assertThat(simpleTestMulti.getI()).isEqualTo(Status.STARTED);
    }
}
