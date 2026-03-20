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
import org.apache.flink.table.data.RowData;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test conversion of proto primitive data to flink internal data. */
class SimpleProtoToRowTest {
    @Test
    void testSimple() throws Exception {
        SimpleTestMulti simple =
                SimpleTestMulti.newBuilder()
                        .setA(1)
                        .setB(2L)
                        .setC(false)
                        .setD(0.1f)
                        .setE(0.01)
                        .setF("haha")
                        .setG(ByteString.copyFrom(new byte[] {1}))
                        .setH(SimpleTestMulti.Corpus.IMAGES)
                        .setI(Status.FINISHED)
                        .setFAbc7D(1) // test fieldNameToJsonName
                        .setVpr6S(2)
                        .build();

        RowData row = ProtobufTestHelper.pbBytesToRow(SimpleTestMulti.class, simple.toByteArray());

        assertThat(row.getArity()).isEqualTo(11);
        assertThat(row.getInt(0)).isEqualTo(1);
        assertThat(row.getLong(1)).isEqualTo(2L);
        assertThat(row.getBoolean(2)).isFalse();
        assertThat(row.getFloat(3)).isEqualTo(0.1f);
        assertThat(row.getDouble(4)).isEqualTo(0.01d);
        assertThat(row.getString(5).toString()).isEqualTo("haha");
        assertThat(row.getBinary(6)[0]).isEqualTo((byte) 1);
        assertThat(row.getString(7).toString()).isEqualTo("IMAGES");
        assertThat(row.getString(8).toString()).isEqualTo("FINISHED");
        assertThat(row.getInt(9)).isEqualTo(1);
        assertThat(row.getInt(10)).isEqualTo(2);
    }

    @Test
    void testNotExistsValueIgnoringDefault() throws Exception {
        SimpleTestMulti simple =
                SimpleTestMulti.newBuilder()
                        .setB(2L)
                        .setC(false)
                        .setD(0.1f)
                        .setE(0.01)
                        .setF("haha")
                        .build();

        RowData row = ProtobufTestHelper.pbBytesToRow(SimpleTestMulti.class, simple.toByteArray());

        assertThat(row.isNullAt(0)).isTrue();
        assertThat(row.isNullAt(1)).isFalse();
    }

    @Test
    void testDefaultValues() throws Exception {
        SimpleTestMulti simple = SimpleTestMulti.newBuilder().build();

        RowData row =
                ProtobufTestHelper.pbBytesToRow(
                        SimpleTestMulti.class,
                        simple.toByteArray(),
                        new PbFormatConfig(SimpleTestMulti.class.getName(), false, true, ""),
                        false);

        assertThat(row.isNullAt(0)).isFalse();
        assertThat(row.isNullAt(1)).isFalse();
        assertThat(row.isNullAt(2)).isFalse();
        assertThat(row.isNullAt(3)).isFalse();
        assertThat(row.isNullAt(4)).isFalse();
        assertThat(row.isNullAt(5)).isFalse();
        assertThat(row.isNullAt(6)).isFalse();
        assertThat(row.isNullAt(7)).isFalse();
        assertThat(row.isNullAt(8)).isFalse();
        assertThat(row.getInt(0)).isEqualTo(10);
        assertThat(row.getLong(1)).isEqualTo(100L);
        assertThat(row.getBoolean(2)).isFalse();
        assertThat(row.getFloat(3)).isEqualTo(0.0f);
        assertThat(row.getDouble(4)).isEqualTo(0.0d);
        assertThat(row.getString(5).toString()).isEqualTo("f");
        assertThat(row.getBinary(6)).isEqualTo(ByteString.EMPTY.toByteArray());
        assertThat(row.getString(7).toString())
                .isEqualTo(SimpleTestMulti.Corpus.UNIVERSAL.toString());
        assertThat(row.getString(8).toString()).isEqualTo(Status.UNSPECIFIED.toString());
    }

    @Test
    void testIntEnum() throws Exception {
        SimpleTestMulti simple =
                SimpleTestMulti.newBuilder()
                        .setH(SimpleTestMulti.Corpus.IMAGES)
                        .setI(Status.STARTED)
                        .build();
        RowData row =
                ProtobufTestHelper.pbBytesToRow(SimpleTestMulti.class, simple.toByteArray(), true);
        assertThat(row.getInt(7)).isEqualTo(2);
        assertThat(row.getInt(8)).isEqualTo(1);
    }
}
