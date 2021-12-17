/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.drivers.transform;

import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.DoubleToLongValueWithProperHashCode;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.DoubleValueToLongValueWithProperHashCode;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.LongValueToChar;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.LongValueToCharValue;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.LongValueToDouble;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.LongValueToDoubleValue;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.LongValueToLong;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.LongValueToLongValueWithProperHashCode;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.LongValueToString;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.LongValueToUnsignedByte;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.LongValueToUnsignedByteValue;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.LongValueToUnsignedFloat;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.LongValueToUnsignedFloatValue;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.LongValueToUnsignedInt;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.LongValueToUnsignedShort;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.LongValueToUnsignedShortValue;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.StringToLongValueWithProperHashCode;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.StringValueToLongValueWithProperHashCode;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.UnsignedByteToLongValueWithProperHashCode;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.UnsignedByteValueToLongValueWithProperHashCode;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.UnsignedFloatToLongValueWithProperHashCode;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.UnsignedFloatValueToLongValueWithProperHashCode;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.UnsignedIntToLongValueWithProperHashCode;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.UnsignedShortToLongValueWithProperHashCode;
import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform.UnsignedShortValueToLongValueWithProperHashCode;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.CharValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.ShortValue;
import org.apache.flink.types.StringValue;

import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link GraphKeyTypeTransform}. */
public class GraphKeyTypeTransformTest {

    private ByteValue byteValue = new ByteValue();
    private ShortValue shortValue = new ShortValue();
    private CharValue charValue = new CharValue();
    private FloatValue floatValue = new FloatValue();
    private DoubleValue doubleValue = new DoubleValue();
    private LongValueWithProperHashCode longValueWithProperHashCode =
            new LongValueWithProperHashCode();

    // ByteValue

    @Test
    public void testToByteValue() throws Exception {
        TranslateFunction<LongValue, ByteValue> translator = new LongValueToUnsignedByteValue();

        Assert.assertEquals(
                new ByteValue((byte) 0), translator.translate(new LongValue(0L), byteValue));

        Assert.assertEquals(
                new ByteValue(Byte.MIN_VALUE),
                translator.translate(new LongValue(Byte.MAX_VALUE + 1), byteValue));

        Assert.assertEquals(
                new ByteValue((byte) -1),
                translator.translate(
                        new LongValue(LongValueToUnsignedByteValue.MAX_VERTEX_COUNT - 1),
                        byteValue));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToByteValueUpperOutOfRange() throws Exception {
        new LongValueToUnsignedByteValue()
                .translate(new LongValue(LongValueToUnsignedByteValue.MAX_VERTEX_COUNT), byteValue);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToByteValueLowerOutOfRange() throws Exception {
        new LongValueToUnsignedByteValue().translate(new LongValue(-1), byteValue);
    }

    @Test
    public void testFromByteValue() throws Exception {
        TranslateFunction<ByteValue, LongValueWithProperHashCode> translator =
                new UnsignedByteValueToLongValueWithProperHashCode();

        Assert.assertEquals(
                new LongValueWithProperHashCode(0L),
                translator.translate(new ByteValue((byte) 0), longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(Byte.MAX_VALUE + 1),
                translator.translate(new ByteValue(Byte.MIN_VALUE), longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(LongValueToUnsignedByteValue.MAX_VERTEX_COUNT - 1),
                translator.translate(new ByteValue((byte) -1), longValueWithProperHashCode));
    }

    // Byte

    @Test
    public void testToByte() throws Exception {
        TranslateFunction<LongValue, Byte> translator = new LongValueToUnsignedByte();

        Assert.assertEquals(Byte.valueOf((byte) 0), translator.translate(new LongValue(0L), null));

        Assert.assertEquals(
                Byte.valueOf(Byte.MIN_VALUE),
                translator.translate(new LongValue((long) Byte.MAX_VALUE + 1), null));

        Assert.assertEquals(
                Byte.valueOf((byte) -1),
                translator.translate(
                        new LongValue(LongValueToUnsignedByte.MAX_VERTEX_COUNT - 1), null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToByteUpperOutOfRange() throws Exception {
        new LongValueToUnsignedByte()
                .translate(new LongValue(LongValueToUnsignedByte.MAX_VERTEX_COUNT), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToByteLowerOutOfRange() throws Exception {
        new LongValueToUnsignedByte().translate(new LongValue(-1), null);
    }

    @Test
    public void testFromByte() throws Exception {
        TranslateFunction<Byte, LongValueWithProperHashCode> translator =
                new UnsignedByteToLongValueWithProperHashCode();

        Assert.assertEquals(
                new LongValueWithProperHashCode(0L),
                translator.translate((byte) 0, longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(Byte.MAX_VALUE + 1),
                translator.translate(Byte.MIN_VALUE, longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(LongValueToUnsignedByte.MAX_VERTEX_COUNT - 1),
                translator.translate((byte) -1, longValueWithProperHashCode));
    }

    // ShortValue

    @Test
    public void testToShortValue() throws Exception {
        TranslateFunction<LongValue, ShortValue> translator = new LongValueToUnsignedShortValue();

        Assert.assertEquals(
                new ShortValue((short) 0), translator.translate(new LongValue(0L), shortValue));

        Assert.assertEquals(
                new ShortValue(Short.MIN_VALUE),
                translator.translate(new LongValue((long) Short.MAX_VALUE + 1), shortValue));

        Assert.assertEquals(
                new ShortValue((short) -1),
                translator.translate(
                        new LongValue(LongValueToUnsignedShortValue.MAX_VERTEX_COUNT - 1),
                        shortValue));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToShortValueUpperOutOfRange() throws Exception {
        new LongValueToUnsignedShortValue()
                .translate(
                        new LongValue(LongValueToUnsignedShortValue.MAX_VERTEX_COUNT), shortValue);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToShortValueLowerOutOfRange() throws Exception {
        new LongValueToUnsignedShortValue().translate(new LongValue(-1), shortValue);
    }

    @Test
    public void testFromShortValue() throws Exception {
        TranslateFunction<ShortValue, LongValueWithProperHashCode> translator =
                new UnsignedShortValueToLongValueWithProperHashCode();

        Assert.assertEquals(
                new LongValueWithProperHashCode(0L),
                translator.translate(new ShortValue((short) 0), longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(Short.MAX_VALUE + 1),
                translator.translate(new ShortValue(Short.MIN_VALUE), longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(LongValueToUnsignedShortValue.MAX_VERTEX_COUNT - 1),
                translator.translate(new ShortValue((short) -1), longValueWithProperHashCode));
    }

    // Short

    @Test
    public void testToShort() throws Exception {
        TranslateFunction<LongValue, Short> translator = new LongValueToUnsignedShort();

        Assert.assertEquals(
                Short.valueOf((short) 0), translator.translate(new LongValue(0L), null));

        Assert.assertEquals(
                Short.valueOf(Short.MIN_VALUE),
                translator.translate(new LongValue((long) Short.MAX_VALUE + 1), null));

        Assert.assertEquals(
                Short.valueOf((short) -1),
                translator.translate(
                        new LongValue(LongValueToUnsignedShort.MAX_VERTEX_COUNT - 1), null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToShortUpperOutOfRange() throws Exception {
        new LongValueToUnsignedShort()
                .translate(new LongValue(LongValueToUnsignedShort.MAX_VERTEX_COUNT), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToShortLowerOutOfRange() throws Exception {
        new LongValueToUnsignedShort().translate(new LongValue(-1), null);
    }

    @Test
    public void testFromShort() throws Exception {
        TranslateFunction<Short, LongValueWithProperHashCode> translator =
                new UnsignedShortToLongValueWithProperHashCode();

        Assert.assertEquals(
                new LongValueWithProperHashCode(0L),
                translator.translate((short) 0, longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(Short.MAX_VALUE + 1),
                translator.translate(Short.MIN_VALUE, longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(LongValueToUnsignedShort.MAX_VERTEX_COUNT - 1),
                translator.translate((short) -1, longValueWithProperHashCode));
    }

    // CharValue

    @Test
    public void testToCharValue() throws Exception {
        TranslateFunction<LongValue, CharValue> translator = new LongValueToCharValue();

        Assert.assertEquals(
                new CharValue((char) 0), translator.translate(new LongValue(0L), charValue));

        Assert.assertEquals(
                new CharValue(Character.MAX_VALUE),
                translator.translate(
                        new LongValue(LongValueToCharValue.MAX_VERTEX_COUNT - 1), charValue));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToCharValueUpperOutOfRange() throws Exception {
        new LongValueToCharValue()
                .translate(new LongValue(LongValueToCharValue.MAX_VERTEX_COUNT), charValue);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToCharValueLowerOutOfRange() throws Exception {
        new LongValueToCharValue().translate(new LongValue(-1), charValue);
    }

    // Character

    @Test
    public void testToCharacter() throws Exception {
        TranslateFunction<LongValue, Character> translator = new LongValueToChar();

        Assert.assertEquals(
                Character.valueOf((char) 0), translator.translate(new LongValue(0L), null));

        Assert.assertEquals(
                Character.valueOf(Character.MAX_VALUE),
                translator.translate(new LongValue(LongValueToChar.MAX_VERTEX_COUNT - 1), null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToCharacterUpperOutOfRange() throws Exception {
        new LongValueToChar().translate(new LongValue(LongValueToChar.MAX_VERTEX_COUNT), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToCharacterLowerOutOfRange() throws Exception {
        new LongValueToChar().translate(new LongValue(-1), null);
    }

    // Integer

    @Test
    public void testToInt() throws Exception {
        TranslateFunction<LongValue, Integer> translator = new LongValueToUnsignedInt();

        Assert.assertEquals(Integer.valueOf(0), translator.translate(new LongValue(0L), null));

        Assert.assertEquals(
                Integer.valueOf(Integer.MIN_VALUE),
                translator.translate(new LongValue((long) Integer.MAX_VALUE + 1), null));

        Assert.assertEquals(
                Integer.valueOf(-1),
                translator.translate(
                        new LongValue(LongValueToUnsignedInt.MAX_VERTEX_COUNT - 1), null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToIntUpperOutOfRange() throws Exception {
        new LongValueToUnsignedInt()
                .translate(new LongValue(LongValueToUnsignedInt.MAX_VERTEX_COUNT), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToIntLowerOutOfRange() throws Exception {
        new LongValueToUnsignedInt().translate(new LongValue(-1), null);
    }

    @Test
    public void testFromInt() throws Exception {
        TranslateFunction<Integer, LongValueWithProperHashCode> translator =
                new UnsignedIntToLongValueWithProperHashCode();

        Assert.assertEquals(
                new LongValueWithProperHashCode(0L),
                translator.translate(0, longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode((long) Integer.MAX_VALUE + 1),
                translator.translate(Integer.MIN_VALUE, longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(LongValueToUnsignedInt.MAX_VERTEX_COUNT - 1),
                translator.translate(-1, longValueWithProperHashCode));
    }

    // LongValue

    @Test
    public void testFromLongValue() throws Exception {
        TranslateFunction<LongValue, LongValueWithProperHashCode> translator =
                new LongValueToLongValueWithProperHashCode();

        Assert.assertEquals(
                new LongValueWithProperHashCode(0L),
                translator.translate(new LongValue(0), longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(Long.MIN_VALUE),
                translator.translate(new LongValue(Long.MIN_VALUE), longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(Long.MAX_VALUE),
                translator.translate(new LongValue(Long.MAX_VALUE), longValueWithProperHashCode));
    }

    // Long

    @Test
    public void testLongValueToLongTranslation() throws Exception {
        TranslateFunction<LongValue, Long> translator = new LongValueToLong();

        Assert.assertEquals(Long.valueOf(0L), translator.translate(new LongValue(0L), null));

        Assert.assertEquals(
                Long.valueOf(Long.MIN_VALUE),
                translator.translate(new LongValue(Long.MIN_VALUE), null));

        Assert.assertEquals(
                Long.valueOf(Long.MAX_VALUE),
                translator.translate(new LongValue(Long.MAX_VALUE), null));
    }

    // FloatValue

    @Test
    public void testToFloatValue() throws Exception {
        TranslateFunction<LongValue, FloatValue> translator = new LongValueToUnsignedFloatValue();

        Assert.assertEquals(
                new FloatValue(Float.intBitsToFloat(0)),
                translator.translate(new LongValue(0L), floatValue));

        Assert.assertEquals(
                new FloatValue(Float.intBitsToFloat(Integer.MIN_VALUE)),
                translator.translate(new LongValue((long) Integer.MAX_VALUE + 1), floatValue));

        Assert.assertEquals(
                new FloatValue(Float.intBitsToFloat(-1)),
                translator.translate(
                        new LongValue(LongValueToUnsignedFloatValue.MAX_VERTEX_COUNT - 1),
                        floatValue));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToFloatValueUpperOutOfRange() throws Exception {
        new LongValueToUnsignedFloatValue()
                .translate(
                        new LongValue(LongValueToUnsignedFloatValue.MAX_VERTEX_COUNT), floatValue);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToFloatValueLowerOutOfRange() throws Exception {
        new LongValueToUnsignedFloatValue().translate(new LongValue(-1), floatValue);
    }

    @Test
    public void testFromFloatValue() throws Exception {
        TranslateFunction<FloatValue, LongValueWithProperHashCode> translator =
                new UnsignedFloatValueToLongValueWithProperHashCode();

        Assert.assertEquals(
                new LongValueWithProperHashCode(0L),
                translator.translate(
                        new FloatValue(Float.intBitsToFloat(0)), longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode((long) Integer.MAX_VALUE + 1),
                translator.translate(
                        new FloatValue(Float.intBitsToFloat(Integer.MIN_VALUE)),
                        longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(LongValueToUnsignedFloatValue.MAX_VERTEX_COUNT - 1),
                translator.translate(
                        new FloatValue(Float.intBitsToFloat(-1)), longValueWithProperHashCode));
    }

    // Float

    @Test
    public void testToFloat() throws Exception {
        TranslateFunction<LongValue, Float> translator = new LongValueToUnsignedFloat();

        Assert.assertEquals(
                Float.valueOf(Float.intBitsToFloat(0)),
                translator.translate(new LongValue(0L), null));

        Assert.assertEquals(
                Float.valueOf(Float.intBitsToFloat(Integer.MIN_VALUE)),
                translator.translate(new LongValue((long) Integer.MAX_VALUE + 1), null));

        Assert.assertEquals(
                Float.valueOf(Float.intBitsToFloat(-1)),
                translator.translate(
                        new LongValue(LongValueToUnsignedFloat.MAX_VERTEX_COUNT - 1), null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToFloatUpperOutOfRange() throws Exception {
        new LongValueToUnsignedFloat()
                .translate(new LongValue(LongValueToUnsignedFloat.MAX_VERTEX_COUNT), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToFloatLowerOutOfRange() throws Exception {
        new LongValueToUnsignedFloat().translate(new LongValue(-1), null);
    }

    @Test
    public void testFromFloat() throws Exception {
        TranslateFunction<Float, LongValueWithProperHashCode> translator =
                new UnsignedFloatToLongValueWithProperHashCode();

        Assert.assertEquals(
                new LongValueWithProperHashCode(0L),
                translator.translate(Float.intBitsToFloat(0), longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode((long) Integer.MAX_VALUE + 1),
                translator.translate(
                        Float.intBitsToFloat(Integer.MIN_VALUE), longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(LongValueToUnsignedFloat.MAX_VERTEX_COUNT - 1),
                translator.translate(Float.intBitsToFloat(-1), longValueWithProperHashCode));
    }

    // DoubleValue

    @Test
    public void testToDoubleValue() throws Exception {
        TranslateFunction<LongValue, DoubleValue> translator = new LongValueToDoubleValue();

        Assert.assertEquals(
                new DoubleValue(Double.longBitsToDouble(0L)),
                translator.translate(new LongValue(0L), doubleValue));

        Assert.assertEquals(
                new DoubleValue(Double.longBitsToDouble(Long.MIN_VALUE)),
                translator.translate(new LongValue(Long.MIN_VALUE), doubleValue));

        Assert.assertEquals(
                new DoubleValue(Double.longBitsToDouble(Long.MAX_VALUE)),
                translator.translate(new LongValue(Long.MAX_VALUE), doubleValue));
    }

    @Test
    public void testFromDoubleValue() throws Exception {
        TranslateFunction<DoubleValue, LongValueWithProperHashCode> translator =
                new DoubleValueToLongValueWithProperHashCode();

        Assert.assertEquals(
                new LongValueWithProperHashCode(0L),
                translator.translate(
                        new DoubleValue(Double.longBitsToDouble(0L)), longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(Long.MIN_VALUE),
                translator.translate(
                        new DoubleValue(Double.longBitsToDouble(Long.MIN_VALUE)),
                        longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(Long.MAX_VALUE),
                translator.translate(
                        new DoubleValue(Double.longBitsToDouble(Long.MAX_VALUE)),
                        longValueWithProperHashCode));
    }

    // Double

    @Test
    public void testToDouble() throws Exception {
        TranslateFunction<LongValue, Double> translator = new LongValueToDouble();

        Assert.assertEquals(
                Double.valueOf(Double.longBitsToDouble(0L)),
                translator.translate(new LongValue(0L), null));

        Assert.assertEquals(
                Double.valueOf(Double.longBitsToDouble(Long.MIN_VALUE)),
                translator.translate(new LongValue(Long.MIN_VALUE), null));

        Assert.assertEquals(
                Double.valueOf(Double.longBitsToDouble(Long.MAX_VALUE)),
                translator.translate(new LongValue(Long.MAX_VALUE), null));
    }

    @Test
    public void testFromDouble() throws Exception {
        TranslateFunction<Double, LongValueWithProperHashCode> translator =
                new DoubleToLongValueWithProperHashCode();

        Assert.assertEquals(
                new LongValueWithProperHashCode(0L),
                translator.translate(Double.longBitsToDouble(0L), longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(Long.MIN_VALUE),
                translator.translate(
                        Double.longBitsToDouble(Long.MIN_VALUE), longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(Long.MAX_VALUE),
                translator.translate(
                        Double.longBitsToDouble(Long.MAX_VALUE), longValueWithProperHashCode));
    }

    // StringValue

    @Test
    public void testFromStringValue() throws Exception {
        TranslateFunction<StringValue, LongValueWithProperHashCode> translator =
                new StringValueToLongValueWithProperHashCode();

        Assert.assertEquals(
                new LongValueWithProperHashCode(0L),
                translator.translate(new StringValue("0"), longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(Long.MIN_VALUE),
                translator.translate(
                        new StringValue("-9223372036854775808"), longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(Long.MAX_VALUE),
                translator.translate(
                        new StringValue("9223372036854775807"), longValueWithProperHashCode));
    }

    // String

    @Test
    public void testLongValueToStringTranslation() throws Exception {
        TranslateFunction<LongValue, String> translator = new LongValueToString();

        Assert.assertEquals("0", translator.translate(new LongValue(0L), null));

        Assert.assertEquals(
                "-9223372036854775808", translator.translate(new LongValue(Long.MIN_VALUE), null));

        Assert.assertEquals(
                "9223372036854775807", translator.translate(new LongValue(Long.MAX_VALUE), null));
    }

    @Test
    public void testFromString() throws Exception {
        TranslateFunction<String, LongValueWithProperHashCode> translator =
                new StringToLongValueWithProperHashCode();

        Assert.assertEquals(
                new LongValueWithProperHashCode(0L),
                translator.translate("0", longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(Long.MIN_VALUE),
                translator.translate("-9223372036854775808", longValueWithProperHashCode));

        Assert.assertEquals(
                new LongValueWithProperHashCode(Long.MAX_VALUE),
                translator.translate("9223372036854775807", longValueWithProperHashCode));
    }
}
