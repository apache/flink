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

package org.apache.flink.api.java.io;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.CharValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.ShortValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the CSV reader builder. */
class CSVReaderTest {

    @Test
    void testIgnoreHeaderConfigure() {
        CsvReader reader = getCsvReader();
        reader.ignoreFirstLine();
        assertThat(reader.skipFirstLineAsHeader).isTrue();
    }

    @Test
    void testIgnoreInvalidLinesConfigure() {
        CsvReader reader = getCsvReader();
        assertThat(reader.ignoreInvalidLines).isFalse();
        reader.ignoreInvalidLines();
        assertThat(reader.ignoreInvalidLines).isTrue();
    }

    @Test
    void testIgnoreComments() {
        CsvReader reader = getCsvReader();
        assertThat(reader.commentPrefix).isNull();
        reader.ignoreComments("#");
        assertThat(reader.commentPrefix).isEqualTo("#");
    }

    @Test
    void testCharset() {
        CsvReader reader = getCsvReader();
        assertThat(reader.getCharset()).isEqualTo("UTF-8");
        reader.setCharset("US-ASCII");
        assertThat(reader.getCharset()).isEqualTo("US-ASCII");
    }

    @Test
    void testIncludeFieldsDense() {
        CsvReader reader = getCsvReader();
        reader.includeFields(true, true, true);
        assertThat(reader.includedMask).containsExactly(true, true, true);

        reader = getCsvReader();
        reader.includeFields("ttt");
        assertThat(reader.includedMask).containsExactly(true, true, true);

        reader = getCsvReader();
        reader.includeFields("TTT");
        assertThat(reader.includedMask).containsExactly(true, true, true);

        reader = getCsvReader();
        reader.includeFields("111");
        assertThat(reader.includedMask).containsExactly(true, true, true);

        reader = getCsvReader();
        reader.includeFields(0x7L);
        assertThat(reader.includedMask).containsExactly(true, true, true);
    }

    @Test
    void testIncludeFieldsSparse() {
        CsvReader reader = getCsvReader();
        reader.includeFields(false, true, true, false, false, true, false, false);
        assertThat(reader.includedMask).containsExactly(false, true, true, false, false, true);

        reader = getCsvReader();
        reader.includeFields("fttfftff");
        assertThat(reader.includedMask).containsExactly(false, true, true, false, false, true);

        reader = getCsvReader();
        reader.includeFields("FTTFFTFF");
        assertThat(reader.includedMask).containsExactly(false, true, true, false, false, true);

        reader = getCsvReader();
        reader.includeFields("01100100");
        assertThat(reader.includedMask).containsExactly(false, true, true, false, false, true);

        reader = getCsvReader();
        reader.includeFields("0t1f0TFF");
        assertThat(reader.includedMask).containsExactly(false, true, true, false, false, true);

        reader = getCsvReader();
        reader.includeFields(0x26L);
        assertThat(reader.includedMask).containsExactly(false, true, true, false, false, true);
    }

    @Test
    void testIllegalCharInStringMask() {
        CsvReader reader = getCsvReader();
        assertThatThrownBy(() -> reader.includeFields("1t0Tfht"))
                .withFailMessage("Reader accepted an invalid mask string")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testIncludeFieldsErrorWhenExcludingAll() {
        CsvReader reader = getCsvReader();

        assertThatThrownBy(() -> reader.includeFields(false, false, false, false, false, false))
                .withFailMessage(
                        "The reader accepted a fields configuration that excludes all fields.")
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> reader.includeFields(0))
                .withFailMessage(
                        "The reader accepted a fields configuration that excludes all fields.")
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> reader.includeFields("ffffffffffffff"))
                .withFailMessage(
                        "The reader accepted a fields configuration that excludes all fields.")
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> reader.includeFields("00000000000000000"))
                .withFailMessage(
                        "The reader accepted a fields configuration that excludes all fields.")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testReturnType() {
        CsvReader reader = getCsvReader();
        DataSource<Item> items = reader.tupleType(Item.class);
        assertThat(items.getType().getTypeClass()).isSameAs(Item.class);
    }

    @Test
    void testFieldTypes() {
        CsvReader reader = getCsvReader();
        DataSource<Item> items = reader.tupleType(Item.class);

        TypeInformation<?> info = items.getType();
        if (!info.isTupleType()) {
            fail("");
        } else {
            TupleTypeInfo<?> tinfo = (TupleTypeInfo<?>) info;
            assertThat(tinfo.getTypeAt(0)).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
            assertThat(tinfo.getTypeAt(1)).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
            assertThat(tinfo.getTypeAt(2)).isEqualTo(BasicTypeInfo.DOUBLE_TYPE_INFO);
            assertThat(tinfo.getTypeAt(3)).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
        }

        CsvInputFormat<?> inputFormat = (CsvInputFormat<?>) items.getInputFormat();
        assertThat(inputFormat.getFieldTypes())
                .containsExactly(Integer.class, String.class, Double.class, String.class);
    }

    @Test
    void testSubClass() {
        CsvReader reader = getCsvReader();
        DataSource<SubItem> sitems = reader.tupleType(SubItem.class);
        TypeInformation<?> info = sitems.getType();

        assertThat(info.isTupleType()).isTrue();
        assertThat(info.getTypeClass()).isEqualTo(SubItem.class);

        @SuppressWarnings("unchecked")
        TupleTypeInfo<SubItem> tinfo = (TupleTypeInfo<SubItem>) info;

        assertThat(tinfo.getTypeAt(0)).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
        assertThat(tinfo.getTypeAt(1)).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
        assertThat(tinfo.getTypeAt(2)).isEqualTo(BasicTypeInfo.DOUBLE_TYPE_INFO);
        assertThat(tinfo.getTypeAt(3)).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);

        CsvInputFormat<?> inputFormat = (CsvInputFormat<?>) sitems.getInputFormat();
        assertThat(inputFormat.getFieldTypes())
                .containsExactly(Integer.class, String.class, Double.class, String.class);
    }

    @Test
    void testSubClassWithPartialsInHierarchie() {
        CsvReader reader = getCsvReader();
        DataSource<FinalItem> sitems = reader.tupleType(FinalItem.class);
        TypeInformation<?> info = sitems.getType();

        assertThat(info.isTupleType()).isTrue();
        assertThat(info.getTypeClass()).isEqualTo(FinalItem.class);

        @SuppressWarnings("unchecked")
        TupleTypeInfo<SubItem> tinfo = (TupleTypeInfo<SubItem>) info;

        assertThat(tinfo.getTypeAt(0)).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
        assertThat(tinfo.getTypeAt(1)).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
        assertThat(tinfo.getTypeAt(2)).isEqualTo(BasicTypeInfo.DOUBLE_TYPE_INFO);
        assertThat(tinfo.getTypeAt(3).getClass()).isEqualTo(ValueTypeInfo.class);
        assertThat(tinfo.getTypeAt(4).getClass()).isEqualTo(ValueTypeInfo.class);
        assertThat((tinfo.getTypeAt(3)).getTypeClass()).isEqualTo(StringValue.class);
        assertThat((tinfo.getTypeAt(4)).getTypeClass()).isEqualTo(LongValue.class);

        CsvInputFormat<?> inputFormat = (CsvInputFormat<?>) sitems.getInputFormat();
        assertThat(inputFormat.getFieldTypes())
                .containsExactly(
                        Integer.class,
                        String.class,
                        Double.class,
                        StringValue.class,
                        LongValue.class);
    }

    @Test
    void testUnsupportedPartialitem() {
        CsvReader reader = getCsvReader();

        assertThatThrownBy(() -> reader.tupleType(PartialItem.class))
                .withFailMessage("tupleType() accepted an underspecified generic class.")
                .isInstanceOf(Exception.class);
    }

    @Test
    void testWithValueType() {
        CsvReader reader = getCsvReader();
        DataSource<
                        Tuple8<
                                StringValue,
                                BooleanValue,
                                ByteValue,
                                ShortValue,
                                IntValue,
                                LongValue,
                                FloatValue,
                                DoubleValue>>
                items =
                        reader.types(
                                StringValue.class,
                                BooleanValue.class,
                                ByteValue.class,
                                ShortValue.class,
                                IntValue.class,
                                LongValue.class,
                                FloatValue.class,
                                DoubleValue.class);
        TypeInformation<?> info = items.getType();

        assertThat(info.isTupleType()).isTrue();
        assertThat(info.getTypeClass()).isEqualTo(Tuple8.class);
    }

    @Test
    void testWithInvalidValueType1() {
        CsvReader reader = getCsvReader();
        // CsvReader doesn't support CharValue
        assertThatThrownBy(() -> reader.types(CharValue.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testWithInvalidValueType2() {
        CsvReader reader = getCsvReader();
        // CsvReader doesn't support custom Value type
        assertThatThrownBy(() -> reader.types(ValueItem.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static CsvReader getCsvReader() {
        return new CsvReader(
                "/some/none/existing/path", ExecutionEnvironment.createLocalEnvironment(1));
    }

    // --------------------------------------------------------------------------------------------
    // Custom types for testing
    // --------------------------------------------------------------------------------------------

    private static class Item extends Tuple4<Integer, String, Double, String> {
        private static final long serialVersionUID = -7444437337392053502L;
    }

    private static class SubItem extends Item {
        private static final long serialVersionUID = 1L;
    }

    private static class PartialItem<A, B, C> extends Tuple5<Integer, A, Double, B, C> {
        private static final long serialVersionUID = 1L;
    }

    private static class FinalItem extends PartialItem<String, StringValue, LongValue> {
        private static final long serialVersionUID = 1L;
    }

    private static class ValueItem implements Value {
        private int v1;

        public int getV1() {
            return v1;
        }

        public void setV1(int v1) {
            this.v1 = v1;
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            out.writeInt(v1);
        }

        @Override
        public void read(DataInputView in) throws IOException {
            v1 = in.readInt();
        }
    }
}
