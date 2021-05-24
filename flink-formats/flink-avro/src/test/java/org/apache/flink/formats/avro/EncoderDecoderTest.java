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

package org.apache.flink.formats.avro;

import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.generated.Colors;
import org.apache.flink.formats.avro.generated.Fixed16;
import org.apache.flink.formats.avro.generated.Fixed2;
import org.apache.flink.formats.avro.generated.User;
import org.apache.flink.formats.avro.utils.DataInputDecoder;
import org.apache.flink.formats.avro.utils.DataOutputEncoder;
import org.apache.flink.util.StringUtils;

import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests the {@link DataOutputEncoder} and {@link DataInputDecoder} classes for Avro serialization.
 */
public class EncoderDecoderTest {

    @Test
    public void testComplexStringsDirecty() {
        try {
            Random rnd = new Random(349712539451944123L);

            for (int i = 0; i < 10; i++) {
                String testString = StringUtils.getRandomString(rnd, 10, 100);

                ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
                {
                    DataOutputStream dataOut = new DataOutputStream(baos);
                    DataOutputEncoder encoder = new DataOutputEncoder();
                    encoder.setOut(dataOut);

                    encoder.writeString(testString);
                    dataOut.flush();
                    dataOut.close();
                }

                byte[] data = baos.toByteArray();

                // deserialize
                {
                    ByteArrayInputStream bais = new ByteArrayInputStream(data);
                    DataInputStream dataIn = new DataInputStream(bais);
                    DataInputDecoder decoder = new DataInputDecoder();
                    decoder.setIn(dataIn);

                    String deserialized = decoder.readString();

                    assertEquals(testString, deserialized);
                }
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test failed due to an exception: " + e.getMessage());
        }
    }

    @Test
    public void testPrimitiveTypes() {

        testObjectSerialization(Boolean.TRUE);
        testObjectSerialization(Boolean.FALSE);

        testObjectSerialization((byte) 0);
        testObjectSerialization((byte) 1);
        testObjectSerialization((byte) -1);
        testObjectSerialization(Byte.MIN_VALUE);
        testObjectSerialization(Byte.MAX_VALUE);

        testObjectSerialization((short) 0);
        testObjectSerialization((short) 1);
        testObjectSerialization((short) -1);
        testObjectSerialization(Short.MIN_VALUE);
        testObjectSerialization(Short.MAX_VALUE);

        testObjectSerialization(0);
        testObjectSerialization(1);
        testObjectSerialization(-1);
        testObjectSerialization(Integer.MIN_VALUE);
        testObjectSerialization(Integer.MAX_VALUE);

        testObjectSerialization(0L);
        testObjectSerialization(1L);
        testObjectSerialization((long) -1);
        testObjectSerialization(Long.MIN_VALUE);
        testObjectSerialization(Long.MAX_VALUE);

        testObjectSerialization(0f);
        testObjectSerialization(1f);
        testObjectSerialization((float) -1);
        testObjectSerialization((float) Math.E);
        testObjectSerialization((float) Math.PI);
        testObjectSerialization(Float.MIN_VALUE);
        testObjectSerialization(Float.MAX_VALUE);
        testObjectSerialization(Float.MIN_NORMAL);
        testObjectSerialization(Float.NaN);
        testObjectSerialization(Float.NEGATIVE_INFINITY);
        testObjectSerialization(Float.POSITIVE_INFINITY);

        testObjectSerialization(0d);
        testObjectSerialization(1d);
        testObjectSerialization((double) -1);
        testObjectSerialization(Math.E);
        testObjectSerialization(Math.PI);
        testObjectSerialization(Double.MIN_VALUE);
        testObjectSerialization(Double.MAX_VALUE);
        testObjectSerialization(Double.MIN_NORMAL);
        testObjectSerialization(Double.NaN);
        testObjectSerialization(Double.NEGATIVE_INFINITY);
        testObjectSerialization(Double.POSITIVE_INFINITY);

        testObjectSerialization("");
        testObjectSerialization("abcdefg");
        testObjectSerialization("ab\u1535\u0155xyz\u706F");

        testObjectSerialization(
                new SimpleTypes(
                        3637,
                        54876486548L,
                        (byte) 65,
                        "We're out looking for astronauts",
                        (short) 0x2387,
                        2.65767523));
        testObjectSerialization(
                new SimpleTypes(
                        705608724,
                        -1L,
                        (byte) -65,
                        "Serve me the sky with a big slice of lemon",
                        (short) Byte.MIN_VALUE,
                        0.0000001));
    }

    @Test
    public void testArrayTypes() {
        {
            int[] array = new int[] {1, 2, 3, 4, 5};
            testObjectSerialization(array);
        }
        {
            long[] array = new long[] {1, 2, 3, 4, 5};
            testObjectSerialization(array);
        }
        {
            float[] array = new float[] {1, 2, 3, 4, 5};
            testObjectSerialization(array);
        }
        {
            double[] array = new double[] {1, 2, 3, 4, 5};
            testObjectSerialization(array);
        }
        {
            String[] array = new String[] {"Oh", "my", "what", "do", "we", "have", "here", "?"};
            testObjectSerialization(array);
        }
    }

    @Test
    public void testEmptyArray() {
        {
            int[] array = new int[0];
            testObjectSerialization(array);
        }
        {
            long[] array = new long[0];
            testObjectSerialization(array);
        }
        {
            float[] array = new float[0];
            testObjectSerialization(array);
        }
        {
            double[] array = new double[0];
            testObjectSerialization(array);
        }
        {
            String[] array = new String[0];
            testObjectSerialization(array);
        }
    }

    @Test
    public void testObjects() {
        // simple object containing only primitives
        {
            testObjectSerialization(new Book(976243875L, "The Serialization Odysse", 42));
        }

        // object with collection
        {
            ArrayList<String> list = new ArrayList<>();
            list.add("A");
            list.add("B");
            list.add("C");
            list.add("D");
            list.add("E");

            testObjectSerialization(new BookAuthor(976243875L, list, "Arno Nym"));
        }

        // object with empty collection
        {
            ArrayList<String> list = new ArrayList<>();
            testObjectSerialization(new BookAuthor(987654321L, list, "The Saurus"));
        }
    }

    @Test
    public void testNestedObjectsWithCollections() {
        testObjectSerialization(new ComplexNestedObject2(true));
    }

    @Test
    public void testGeneratedObjectWithNullableFields() {
        List<CharSequence> strings =
                Arrays.asList(
                        new CharSequence[] {
                            "These",
                            "strings",
                            "should",
                            "be",
                            "recognizable",
                            "as",
                            "a",
                            "meaningful",
                            "sequence"
                        });
        List<Boolean> bools = Arrays.asList(true, true, false, false, true, false, true, true);
        Map<CharSequence, Long> map = new HashMap<>();
        map.put("1", 1L);
        map.put("2", 2L);
        map.put("3", 3L);

        byte[] b = new byte[16];
        new Random().nextBytes(b);
        Fixed16 f = new Fixed16(b);
        Address addr = new Address(239, "6th Main", "Bangalore", "Karnataka", "560075");
        User user =
                new User(
                        "Freudenreich",
                        1337,
                        "macintosh gray",
                        1234567890L,
                        3.1415926,
                        null,
                        true,
                        strings,
                        bools,
                        null,
                        Colors.GREEN,
                        map,
                        f,
                        Boolean.TRUE,
                        addr,
                        ByteBuffer.wrap(b),
                        LocalDate.parse("2014-03-01"),
                        LocalTime.parse("12:12:12"),
                        LocalTime.ofSecondOfDay(0).plus(123456L, ChronoUnit.MICROS),
                        Instant.parse("2014-03-01T12:12:12.321Z"),
                        Instant.ofEpochSecond(0).plus(123456L, ChronoUnit.MICROS),
                        ByteBuffer.wrap(
                                BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()), // 20.00
                        new Fixed2(
                                BigDecimal.valueOf(2000, 2)
                                        .unscaledValue()
                                        .toByteArray())); // 20.00

        testObjectSerialization(user);
    }

    @Test
    public void testVarLenCountEncoding() {
        try {
            long[] values =
                    new long[] {
                        0,
                        1,
                        2,
                        3,
                        4,
                        0,
                        574,
                        45236,
                        0,
                        234623462,
                        23462462346L,
                        0,
                        9734028767869761L,
                        0x7fffffffffffffffL
                    };

            // write
            ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
            {
                DataOutputStream dataOut = new DataOutputStream(baos);

                for (long val : values) {
                    DataOutputEncoder.writeVarLongCount(dataOut, val);
                }

                dataOut.flush();
                dataOut.close();
            }

            // read
            {
                ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
                DataInputStream dataIn = new DataInputStream(bais);

                for (long val : values) {
                    long read = DataInputDecoder.readVarLongCount(dataIn);
                    assertEquals("Wrong var-len encoded value read.", val, read);
                }
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test failed due to an exception: " + e.getMessage());
        }
    }

    private static <X> void testObjectSerialization(X obj) {

        try {

            // serialize
            ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
            {
                DataOutputStream dataOut = new DataOutputStream(baos);
                DataOutputEncoder encoder = new DataOutputEncoder();
                encoder.setOut(dataOut);

                @SuppressWarnings("unchecked")
                Class<X> clazz = (Class<X>) obj.getClass();
                ReflectDatumWriter<X> writer = new ReflectDatumWriter<>(clazz);

                writer.write(obj, encoder);
                dataOut.flush();
                dataOut.close();
            }

            byte[] data = baos.toByteArray();
            X result;

            // deserialize
            {
                ByteArrayInputStream bais = new ByteArrayInputStream(data);
                DataInputStream dataIn = new DataInputStream(bais);
                DataInputDecoder decoder = new DataInputDecoder();
                decoder.setIn(dataIn);

                @SuppressWarnings("unchecked")
                Class<X> clazz = (Class<X>) obj.getClass();
                ReflectDatumReader<X> reader = new ReflectDatumReader<>(clazz);

                // create a reuse object if possible, otherwise we have no reuse object
                X reuse = null;
                try {
                    @SuppressWarnings("unchecked")
                    X test = (X) obj.getClass().newInstance();
                    reuse = test;
                } catch (Throwable t) {
                    // do nothing
                }

                result = reader.read(reuse, decoder);
            }

            // check
            final String message = "Deserialized object is not the same as the original";

            if (obj.getClass().isArray()) {
                Class<?> clazz = obj.getClass();
                if (clazz == byte[].class) {
                    assertArrayEquals(message, (byte[]) obj, (byte[]) result);
                } else if (clazz == short[].class) {
                    assertArrayEquals(message, (short[]) obj, (short[]) result);
                } else if (clazz == int[].class) {
                    assertArrayEquals(message, (int[]) obj, (int[]) result);
                } else if (clazz == long[].class) {
                    assertArrayEquals(message, (long[]) obj, (long[]) result);
                } else if (clazz == char[].class) {
                    assertArrayEquals(message, (char[]) obj, (char[]) result);
                } else if (clazz == float[].class) {
                    assertArrayEquals(message, (float[]) obj, (float[]) result, 0.0f);
                } else if (clazz == double[].class) {
                    assertArrayEquals(message, (double[]) obj, (double[]) result, 0.0);
                } else {
                    assertArrayEquals(message, (Object[]) obj, (Object[]) result);
                }
            } else {
                assertEquals(message, obj, result);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test failed due to an exception: " + e.getMessage());
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Test Objects
    // --------------------------------------------------------------------------------------------

    private static final class SimpleTypes {

        private final int iVal;
        private final long lVal;
        private final byte bVal;
        private final String sVal;
        private final short rVal;
        private final double dVal;

        public SimpleTypes() {
            this(0, 0, (byte) 0, "", (short) 0, 0);
        }

        public SimpleTypes(int iVal, long lVal, byte bVal, String sVal, short rVal, double dVal) {
            this.iVal = iVal;
            this.lVal = lVal;
            this.bVal = bVal;
            this.sVal = sVal;
            this.rVal = rVal;
            this.dVal = dVal;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj.getClass() == SimpleTypes.class) {
                SimpleTypes other = (SimpleTypes) obj;

                return other.iVal == this.iVal
                        && other.lVal == this.lVal
                        && other.bVal == this.bVal
                        && other.sVal.equals(this.sVal)
                        && other.rVal == this.rVal
                        && other.dVal == this.dVal;

            } else {
                return false;
            }
        }
    }

    private static class ComplexNestedObject1 {

        private double doubleValue;

        private List<String> stringList;

        public ComplexNestedObject1() {}

        public ComplexNestedObject1(int offInit) {
            this.doubleValue = 6293485.6723 + offInit;

            this.stringList = new ArrayList<>();
            this.stringList.add("A" + offInit);
            this.stringList.add("somewhat" + offInit);
            this.stringList.add("random" + offInit);
            this.stringList.add("collection" + offInit);
            this.stringList.add("of" + offInit);
            this.stringList.add("strings" + offInit);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj.getClass() == ComplexNestedObject1.class) {
                ComplexNestedObject1 other = (ComplexNestedObject1) obj;
                return other.doubleValue == this.doubleValue
                        && this.stringList.equals(other.stringList);
            } else {
                return false;
            }
        }
    }

    private static class ComplexNestedObject2 {

        private long longValue;

        private Map<String, ComplexNestedObject1> theMap;

        public ComplexNestedObject2() {}

        public ComplexNestedObject2(boolean init) {
            this.longValue = 46547;

            this.theMap = new HashMap<>();
            this.theMap.put("36354L", new ComplexNestedObject1(43546543));
            this.theMap.put("785611L", new ComplexNestedObject1(45784568));
            this.theMap.put("43L", new ComplexNestedObject1(9876543));
            this.theMap.put("-45687L", new ComplexNestedObject1(7897615));
            this.theMap.put("1919876876896L", new ComplexNestedObject1(27154));
            this.theMap.put("-868468468L", new ComplexNestedObject1(546435));
        }

        @Override
        public boolean equals(Object obj) {
            if (obj.getClass() == ComplexNestedObject2.class) {
                ComplexNestedObject2 other = (ComplexNestedObject2) obj;
                return other.longValue == this.longValue && this.theMap.equals(other.theMap);
            } else {
                return false;
            }
        }
    }

    private static class Book {

        private long bookId;
        private String title;
        private long authorId;

        public Book() {}

        public Book(long bookId, String title, long authorId) {
            this.bookId = bookId;
            this.title = title;
            this.authorId = authorId;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj.getClass() == Book.class) {
                Book other = (Book) obj;
                return other.bookId == this.bookId
                        && other.authorId == this.authorId
                        && this.title.equals(other.title);
            } else {
                return false;
            }
        }
    }

    private static class BookAuthor {

        private long authorId;
        private List<String> bookTitles;
        private String authorName;

        public BookAuthor() {}

        public BookAuthor(long authorId, List<String> bookTitles, String authorName) {
            this.authorId = authorId;
            this.bookTitles = bookTitles;
            this.authorName = authorName;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj.getClass() == BookAuthor.class) {
                BookAuthor other = (BookAuthor) obj;
                return other.authorName.equals(this.authorName)
                        && other.authorId == this.authorId
                        && other.bookTitles.equals(this.bookTitles);
            } else {
                return false;
            }
        }
    }
}
