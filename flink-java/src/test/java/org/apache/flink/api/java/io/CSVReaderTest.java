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

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** Tests for the CSV reader builder. */
public class CSVReaderTest {

    @Test
    public void testIgnoreHeaderConfigure() {
        CsvReader reader = getCsvReader();
        reader.ignoreFirstLine();
        Assert.assertTrue(reader.skipFirstLineAsHeader);
    }

    @Test
    public void testIgnoreInvalidLinesConfigure() {
        CsvReader reader = getCsvReader();
        Assert.assertFalse(reader.ignoreInvalidLines);
        reader.ignoreInvalidLines();
        Assert.assertTrue(reader.ignoreInvalidLines);
    }

    @Test
    public void testIgnoreComments() {
        CsvReader reader = getCsvReader();
        assertNull(reader.commentPrefix);
        reader.ignoreComments("#");
        assertEquals("#", reader.commentPrefix);
    }

    @Test
    public void testCharset() {
        CsvReader reader = getCsvReader();
        assertEquals("UTF-8", reader.getCharset());
        reader.setCharset("US-ASCII");
        assertEquals("US-ASCII", reader.getCharset());
    }

    @Test
    public void testIncludeFieldsDense() {
        CsvReader reader = getCsvReader();
        reader.includeFields(true, true, true);
        Assert.assertTrue(Arrays.equals(new boolean[] {true, true, true}, reader.includedMask));

        reader = getCsvReader();
        reader.includeFields("ttt");
        Assert.assertTrue(Arrays.equals(new boolean[] {true, true, true}, reader.includedMask));

        reader = getCsvReader();
        reader.includeFields("TTT");
        Assert.assertTrue(Arrays.equals(new boolean[] {true, true, true}, reader.includedMask));

        reader = getCsvReader();
        reader.includeFields("111");
        Assert.assertTrue(Arrays.equals(new boolean[] {true, true, true}, reader.includedMask));

        reader = getCsvReader();
        reader.includeFields(0x7L);
        Assert.assertTrue(Arrays.equals(new boolean[] {true, true, true}, reader.includedMask));
    }

    @Test
    public void testIncludeFieldsSparse() {
        CsvReader reader = getCsvReader();
        reader.includeFields(false, true, true, false, false, true, false, false);
        Assert.assertTrue(
                Arrays.equals(
                        new boolean[] {false, true, true, false, false, true},
                        reader.includedMask));

        reader = getCsvReader();
        reader.includeFields("fttfftff");
        Assert.assertTrue(
                Arrays.equals(
                        new boolean[] {false, true, true, false, false, true},
                        reader.includedMask));

        reader = getCsvReader();
        reader.includeFields("FTTFFTFF");
        Assert.assertTrue(
                Arrays.equals(
                        new boolean[] {false, true, true, false, false, true},
                        reader.includedMask));

        reader = getCsvReader();
        reader.includeFields("01100100");
        Assert.assertTrue(
                Arrays.equals(
                        new boolean[] {false, true, true, false, false, true},
                        reader.includedMask));

        reader = getCsvReader();
        reader.includeFields("0t1f0TFF");
        Assert.assertTrue(
                Arrays.equals(
                        new boolean[] {false, true, true, false, false, true},
                        reader.includedMask));

        reader = getCsvReader();
        reader.includeFields(0x26L);
        Assert.assertTrue(
                Arrays.equals(
                        new boolean[] {false, true, true, false, false, true},
                        reader.includedMask));
    }

    @Test
    public void testIllegalCharInStringMask() {
        CsvReader reader = getCsvReader();

        try {
            reader.includeFields("1t0Tfht");
            Assert.fail("Reader accepted an invalid mask string");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testIncludeFieldsErrorWhenExcludingAll() {
        CsvReader reader = getCsvReader();

        try {
            reader.includeFields(false, false, false, false, false, false);
            Assert.fail("The reader accepted a fields configuration that excludes all fields.");
        } catch (IllegalArgumentException e) {
            // all good
        }

        try {
            reader.includeFields(0);
            Assert.fail("The reader accepted a fields configuration that excludes all fields.");
        } catch (IllegalArgumentException e) {
            // all good
        }

        try {
            reader.includeFields("ffffffffffffff");
            Assert.fail("The reader accepted a fields configuration that excludes all fields.");
        } catch (IllegalArgumentException e) {
            // all good
        }

        try {
            reader.includeFields("00000000000000000");
            Assert.fail("The reader accepted a fields configuration that excludes all fields.");
        } catch (IllegalArgumentException e) {
            // all good
        }
    }

    @Test
    public void testReturnType() throws Exception {
        CsvReader reader = getCsvReader();
        DataSource<Item> items = reader.tupleType(Item.class);
        Assert.assertTrue(items.getType().getTypeClass() == Item.class);
    }

    @Test
    public void testFieldTypes() throws Exception {
        CsvReader reader = getCsvReader();
        DataSource<Item> items = reader.tupleType(Item.class);

        TypeInformation<?> info = items.getType();
        if (!info.isTupleType()) {
            Assert.fail();
        } else {
            TupleTypeInfo<?> tinfo = (TupleTypeInfo<?>) info;
            Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tinfo.getTypeAt(0));
            Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tinfo.getTypeAt(1));
            Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, tinfo.getTypeAt(2));
            Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tinfo.getTypeAt(3));
        }

        CsvInputFormat<?> inputFormat = (CsvInputFormat<?>) items.getInputFormat();
        Assert.assertArrayEquals(
                new Class<?>[] {Integer.class, String.class, Double.class, String.class},
                inputFormat.getFieldTypes());
    }

    @Test
    public void testSubClass() throws Exception {
        CsvReader reader = getCsvReader();
        DataSource<SubItem> sitems = reader.tupleType(SubItem.class);
        TypeInformation<?> info = sitems.getType();

        Assert.assertEquals(true, info.isTupleType());
        Assert.assertEquals(SubItem.class, info.getTypeClass());

        @SuppressWarnings("unchecked")
        TupleTypeInfo<SubItem> tinfo = (TupleTypeInfo<SubItem>) info;

        Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tinfo.getTypeAt(0));
        Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tinfo.getTypeAt(1));
        Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, tinfo.getTypeAt(2));
        Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tinfo.getTypeAt(3));

        CsvInputFormat<?> inputFormat = (CsvInputFormat<?>) sitems.getInputFormat();
        Assert.assertArrayEquals(
                new Class<?>[] {Integer.class, String.class, Double.class, String.class},
                inputFormat.getFieldTypes());
    }

    @Test
    public void testSubClassWithPartialsInHierarchie() throws Exception {
        CsvReader reader = getCsvReader();
        DataSource<FinalItem> sitems = reader.tupleType(FinalItem.class);
        TypeInformation<?> info = sitems.getType();

        Assert.assertEquals(true, info.isTupleType());
        Assert.assertEquals(FinalItem.class, info.getTypeClass());

        @SuppressWarnings("unchecked")
        TupleTypeInfo<SubItem> tinfo = (TupleTypeInfo<SubItem>) info;

        Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tinfo.getTypeAt(0));
        Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tinfo.getTypeAt(1));
        Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, tinfo.getTypeAt(2));
        Assert.assertEquals(ValueTypeInfo.class, tinfo.getTypeAt(3).getClass());
        Assert.assertEquals(ValueTypeInfo.class, tinfo.getTypeAt(4).getClass());
        Assert.assertEquals(
                StringValue.class, ((ValueTypeInfo<?>) tinfo.getTypeAt(3)).getTypeClass());
        Assert.assertEquals(
                LongValue.class, ((ValueTypeInfo<?>) tinfo.getTypeAt(4)).getTypeClass());

        CsvInputFormat<?> inputFormat = (CsvInputFormat<?>) sitems.getInputFormat();
        Assert.assertArrayEquals(
                new Class<?>[] {
                    Integer.class, String.class, Double.class, StringValue.class, LongValue.class
                },
                inputFormat.getFieldTypes());
    }

    @Test
    public void testUnsupportedPartialitem() throws Exception {
        CsvReader reader = getCsvReader();

        try {
            reader.tupleType(PartialItem.class);
            Assert.fail("tupleType() accepted an underspecified generic class.");
        } catch (Exception e) {
            // okay.
        }
    }

    @Test
    public void testWithValueType() throws Exception {
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

        Assert.assertEquals(true, info.isTupleType());
        Assert.assertEquals(Tuple8.class, info.getTypeClass());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithInvalidValueType1() throws Exception {
        CsvReader reader = getCsvReader();
        // CsvReader doesn't support CharValue
        reader.types(CharValue.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithInvalidValueType2() throws Exception {
        CsvReader reader = getCsvReader();
        // CsvReader doesn't support custom Value type
        reader.types(ValueItem.class);
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
