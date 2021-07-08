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

package org.apache.flink.test.operators.util;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import org.apache.hadoop.io.IntWritable;

import java.io.File;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.math.BigInt;

/**
 * #######################################################################################################
 * BE AWARE THAT OTHER TESTS DEPEND ON THIS TEST DATA. IF YOU MODIFY THE DATA MAKE SURE YOU CHECK
 * THAT ALL TESTS ARE STILL WORKING!
 * #######################################################################################################
 */
public class CollectionDataSets {

    public static DataSet<Tuple3<Integer, Long, String>> get3TupleDataSet(
            ExecutionEnvironment env) {

        List<Tuple3<Integer, Long, String>> data = new ArrayList<>();
        data.add(new Tuple3<>(1, 1L, "Hi"));
        data.add(new Tuple3<>(2, 2L, "Hello"));
        data.add(new Tuple3<>(3, 2L, "Hello world"));
        data.add(new Tuple3<>(4, 3L, "Hello world, how are you?"));
        data.add(new Tuple3<>(5, 3L, "I am fine."));
        data.add(new Tuple3<>(6, 3L, "Luke Skywalker"));
        data.add(new Tuple3<>(7, 4L, "Comment#1"));
        data.add(new Tuple3<>(8, 4L, "Comment#2"));
        data.add(new Tuple3<>(9, 4L, "Comment#3"));
        data.add(new Tuple3<>(10, 4L, "Comment#4"));
        data.add(new Tuple3<>(11, 5L, "Comment#5"));
        data.add(new Tuple3<>(12, 5L, "Comment#6"));
        data.add(new Tuple3<>(13, 5L, "Comment#7"));
        data.add(new Tuple3<>(14, 5L, "Comment#8"));
        data.add(new Tuple3<>(15, 5L, "Comment#9"));
        data.add(new Tuple3<>(16, 6L, "Comment#10"));
        data.add(new Tuple3<>(17, 6L, "Comment#11"));
        data.add(new Tuple3<>(18, 6L, "Comment#12"));
        data.add(new Tuple3<>(19, 6L, "Comment#13"));
        data.add(new Tuple3<>(20, 6L, "Comment#14"));
        data.add(new Tuple3<>(21, 6L, "Comment#15"));

        Collections.shuffle(data);

        return env.fromCollection(data);
    }

    public static DataSet<Tuple3<Integer, Long, String>> getSmall3TupleDataSet(
            ExecutionEnvironment env) {

        List<Tuple3<Integer, Long, String>> data = new ArrayList<>();
        data.add(new Tuple3<>(1, 1L, "Hi"));
        data.add(new Tuple3<>(2, 2L, "Hello"));
        data.add(new Tuple3<>(3, 2L, "Hello world"));

        Collections.shuffle(data);

        return env.fromCollection(data);
    }

    public static DataSet<Tuple5<Integer, Long, Integer, String, Long>> get5TupleDataSet(
            ExecutionEnvironment env) {

        List<Tuple5<Integer, Long, Integer, String, Long>> data = new ArrayList<>();
        data.add(new Tuple5<>(1, 1L, 0, "Hallo", 1L));
        data.add(new Tuple5<>(2, 2L, 1, "Hallo Welt", 2L));
        data.add(new Tuple5<>(2, 3L, 2, "Hallo Welt wie", 1L));
        data.add(new Tuple5<>(3, 4L, 3, "Hallo Welt wie gehts?", 2L));
        data.add(new Tuple5<>(3, 5L, 4, "ABC", 2L));
        data.add(new Tuple5<>(3, 6L, 5, "BCD", 3L));
        data.add(new Tuple5<>(4, 7L, 6, "CDE", 2L));
        data.add(new Tuple5<>(4, 8L, 7, "DEF", 1L));
        data.add(new Tuple5<>(4, 9L, 8, "EFG", 1L));
        data.add(new Tuple5<>(4, 10L, 9, "FGH", 2L));
        data.add(new Tuple5<>(5, 11L, 10, "GHI", 1L));
        data.add(new Tuple5<>(5, 12L, 11, "HIJ", 3L));
        data.add(new Tuple5<>(5, 13L, 12, "IJK", 3L));
        data.add(new Tuple5<>(5, 14L, 13, "JKL", 2L));
        data.add(new Tuple5<>(5, 15L, 14, "KLM", 2L));

        Collections.shuffle(data);

        TupleTypeInfo<Tuple5<Integer, Long, Integer, String, Long>> type =
                new TupleTypeInfo<>(
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.LONG_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.LONG_TYPE_INFO);

        return env.fromCollection(data, type);
    }

    public static DataSet<Tuple5<Integer, Long, Integer, String, Long>> getSmall5TupleDataSet(
            ExecutionEnvironment env) {

        List<Tuple5<Integer, Long, Integer, String, Long>> data = new ArrayList<>();
        data.add(new Tuple5<>(1, 1L, 0, "Hallo", 1L));
        data.add(new Tuple5<>(2, 2L, 1, "Hallo Welt", 2L));
        data.add(new Tuple5<>(2, 3L, 2, "Hallo Welt wie", 1L));

        Collections.shuffle(data);

        TupleTypeInfo<Tuple5<Integer, Long, Integer, String, Long>> type =
                new TupleTypeInfo<>(
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.LONG_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.LONG_TYPE_INFO);

        return env.fromCollection(data, type);
    }

    public static DataSet<Tuple2<Tuple2<Integer, Integer>, String>> getSmallNestedTupleDataSet(
            ExecutionEnvironment env) {

        List<Tuple2<Tuple2<Integer, Integer>, String>> data = new ArrayList<>();
        data.add(new Tuple2<>(new Tuple2<>(1, 1), "one"));
        data.add(new Tuple2<>(new Tuple2<>(2, 2), "two"));
        data.add(new Tuple2<>(new Tuple2<>(3, 3), "three"));

        TupleTypeInfo<Tuple2<Tuple2<Integer, Integer>, String>> type =
                new TupleTypeInfo<>(
                        new TupleTypeInfo<Tuple2<Integer, Integer>>(
                                BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO),
                        BasicTypeInfo.STRING_TYPE_INFO);

        return env.fromCollection(data, type);
    }

    public static DataSet<Tuple2<Tuple2<Integer, Integer>, String>>
            getGroupSortedNestedTupleDataSet(ExecutionEnvironment env) {

        List<Tuple2<Tuple2<Integer, Integer>, String>> data = new ArrayList<>();
        data.add(new Tuple2<>(new Tuple2<>(1, 3), "a"));
        data.add(new Tuple2<>(new Tuple2<>(1, 2), "a"));
        data.add(new Tuple2<>(new Tuple2<>(2, 1), "a"));
        data.add(new Tuple2<>(new Tuple2<>(2, 2), "b"));
        data.add(new Tuple2<>(new Tuple2<>(3, 3), "c"));
        data.add(new Tuple2<>(new Tuple2<>(3, 6), "c"));
        data.add(new Tuple2<>(new Tuple2<>(4, 9), "c"));

        TupleTypeInfo<Tuple2<Tuple2<Integer, Integer>, String>> type =
                new TupleTypeInfo<>(
                        new TupleTypeInfo<Tuple2<Integer, Integer>>(
                                BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO),
                        BasicTypeInfo.STRING_TYPE_INFO);

        return env.fromCollection(data, type);
    }

    public static DataSet<Tuple3<Tuple2<Integer, Integer>, String, Integer>>
            getGroupSortedNestedTupleDataSet2(ExecutionEnvironment env) {

        List<Tuple3<Tuple2<Integer, Integer>, String, Integer>> data = new ArrayList<>();
        data.add(new Tuple3<>(new Tuple2<>(1, 3), "a", 2));
        data.add(new Tuple3<>(new Tuple2<>(1, 2), "a", 1));
        data.add(new Tuple3<>(new Tuple2<>(2, 1), "a", 3));
        data.add(new Tuple3<>(new Tuple2<>(2, 2), "b", 4));
        data.add(new Tuple3<>(new Tuple2<>(3, 3), "c", 5));
        data.add(new Tuple3<>(new Tuple2<>(3, 6), "c", 6));
        data.add(new Tuple3<>(new Tuple2<>(4, 9), "c", 7));

        TupleTypeInfo<Tuple3<Tuple2<Integer, Integer>, String, Integer>> type =
                new TupleTypeInfo<>(
                        new TupleTypeInfo<Tuple2<Integer, Integer>>(
                                BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO),
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO);

        return env.fromCollection(data, type);
    }

    public static DataSet<Tuple2<byte[], Integer>> getTuple2WithByteArrayDataSet(
            ExecutionEnvironment env) {
        List<Tuple2<byte[], Integer>> data = new ArrayList<>();
        data.add(new Tuple2<>(new byte[] {0, 4}, 1));
        data.add(new Tuple2<>(new byte[] {2, 0}, 1));
        data.add(new Tuple2<>(new byte[] {2, 0, 4}, 4));
        data.add(new Tuple2<>(new byte[] {2, 1}, 3));
        data.add(new Tuple2<>(new byte[] {0}, 0));
        data.add(new Tuple2<>(new byte[] {2, 0}, 1));

        TupleTypeInfo<Tuple2<byte[], Integer>> type =
                new TupleTypeInfo<>(
                        PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO);

        return env.fromCollection(data, type);
    }

    public static DataSet<String> getStringDataSet(ExecutionEnvironment env) {

        List<String> data = new ArrayList<>();
        data.add("Hi");
        data.add("Hello");
        data.add("Hello world");
        data.add("Hello world, how are you?");
        data.add("I am fine.");
        data.add("Luke Skywalker");
        data.add("Random comment");
        data.add("LOL");

        Collections.shuffle(data);

        return env.fromCollection(data);
    }

    public static DataSet<Integer> getIntegerDataSet(ExecutionEnvironment env) {

        List<Integer> data = new ArrayList<>();
        data.add(1);
        data.add(2);
        data.add(2);
        data.add(3);
        data.add(3);
        data.add(3);
        data.add(4);
        data.add(4);
        data.add(4);
        data.add(4);
        data.add(5);
        data.add(5);
        data.add(5);
        data.add(5);
        data.add(5);

        Collections.shuffle(data);

        return env.fromCollection(data);
    }

    public static DataSet<CustomType> getCustomTypeDataSet(ExecutionEnvironment env) {

        List<CustomType> data = new ArrayList<>();
        data.add(new CustomType(1, 0L, "Hi"));
        data.add(new CustomType(2, 1L, "Hello"));
        data.add(new CustomType(2, 2L, "Hello world"));
        data.add(new CustomType(3, 3L, "Hello world, how are you?"));
        data.add(new CustomType(3, 4L, "I am fine."));
        data.add(new CustomType(3, 5L, "Luke Skywalker"));
        data.add(new CustomType(4, 6L, "Comment#1"));
        data.add(new CustomType(4, 7L, "Comment#2"));
        data.add(new CustomType(4, 8L, "Comment#3"));
        data.add(new CustomType(4, 9L, "Comment#4"));
        data.add(new CustomType(5, 10L, "Comment#5"));
        data.add(new CustomType(5, 11L, "Comment#6"));
        data.add(new CustomType(5, 12L, "Comment#7"));
        data.add(new CustomType(5, 13L, "Comment#8"));
        data.add(new CustomType(5, 14L, "Comment#9"));
        data.add(new CustomType(6, 15L, "Comment#10"));
        data.add(new CustomType(6, 16L, "Comment#11"));
        data.add(new CustomType(6, 17L, "Comment#12"));
        data.add(new CustomType(6, 18L, "Comment#13"));
        data.add(new CustomType(6, 19L, "Comment#14"));
        data.add(new CustomType(6, 20L, "Comment#15"));

        Collections.shuffle(data);

        return env.fromCollection(data);
    }

    public static DataSet<CustomType> getSmallCustomTypeDataSet(ExecutionEnvironment env) {

        List<CustomType> data = new ArrayList<>();
        data.add(new CustomType(1, 0L, "Hi"));
        data.add(new CustomType(2, 1L, "Hello"));
        data.add(new CustomType(2, 2L, "Hello world"));

        Collections.shuffle(data);

        return env.fromCollection(data);
    }

    /** POJO. */
    public static class CustomType implements Serializable {

        private static final long serialVersionUID = 1L;

        public int myInt;
        public long myLong;
        public String myString;

        public CustomType() {}

        public CustomType(int i, long l, String s) {
            myInt = i;
            myLong = l;
            myString = s;
        }

        @Override
        public String toString() {
            return myInt + "," + myLong + "," + myString;
        }
    }

    private static class CustomTypeComparator implements Comparator<CustomType> {
        @Override
        public int compare(CustomType o1, CustomType o2) {
            int diff = o1.myInt - o2.myInt;
            if (diff != 0) {
                return diff;
            }
            diff = (int) (o1.myLong - o2.myLong);
            return diff != 0 ? diff : o1.myString.compareTo(o2.myString);
        }
    }

    public static DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>>
            getSmallTuplebasedDataSet(ExecutionEnvironment env) {
        List<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> data =
                new ArrayList<>();
        data.add(new Tuple7<>(1, "First", 10, 100, 1000L, "One", 10000L));
        data.add(new Tuple7<>(2, "Second", 20, 200, 2000L, "Two", 20000L));
        data.add(new Tuple7<>(3, "Third", 30, 300, 3000L, "Three", 30000L));
        return env.fromCollection(data);
    }

    public static DataSet<Tuple7<Long, Integer, Integer, Long, String, Integer, String>>
            getSmallTuplebasedDataSetMatchingPojo(ExecutionEnvironment env) {
        List<Tuple7<Long, Integer, Integer, Long, String, Integer, String>> data =
                new ArrayList<>();
        data.add(new Tuple7<>(10000L, 10, 100, 1000L, "One", 1, "First"));
        data.add(new Tuple7<>(20000L, 20, 200, 2000L, "Two", 2, "Second"));
        data.add(new Tuple7<>(30000L, 30, 300, 3000L, "Three", 3, "Third"));

        return env.fromCollection(data);
    }

    public static DataSet<POJO> getSmallPojoDataSet(ExecutionEnvironment env) {
        List<POJO> data = new ArrayList<>();
        data.add(
                new POJO(
                        1 /*number*/,
                        "First" /*str*/,
                        10 /*f0*/,
                        100 /*f1.myInt*/,
                        1000L /*f1.myLong*/,
                        "One" /*f1.myString*/,
                        10000L /*nestedPojo.longNumber*/));
        data.add(new POJO(2, "Second", 20, 200, 2000L, "Two", 20000L));
        data.add(new POJO(3, "Third", 30, 300, 3000L, "Three", 30000L));
        return env.fromCollection(data);
    }

    public static DataSet<POJO> getDuplicatePojoDataSet(ExecutionEnvironment env) {
        List<POJO> data = new ArrayList<>();
        data.add(new POJO(1, "First", 10, 100, 1000L, "One", 10000L)); // 5x
        data.add(new POJO(1, "First", 10, 100, 1000L, "One", 10000L));
        data.add(new POJO(1, "First", 10, 100, 1000L, "One", 10000L));
        data.add(new POJO(1, "First", 10, 100, 1000L, "One", 10000L));
        data.add(new POJO(1, "First", 10, 100, 1000L, "One", 10000L));
        data.add(new POJO(2, "Second", 20, 200, 2000L, "Two", 20000L));
        data.add(new POJO(3, "Third", 30, 300, 3000L, "Three", 30000L)); // 2x
        data.add(new POJO(3, "Third", 30, 300, 3000L, "Three", 30000L));
        return env.fromCollection(data);
    }

    public static DataSet<POJO> getMixedPojoDataSet(ExecutionEnvironment env) {
        List<POJO> data = new ArrayList<>();
        data.add(new POJO(1, "First", 10, 100, 1000L, "One", 10100L)); // 5x
        data.add(new POJO(2, "First_", 10, 105, 1000L, "One", 10200L));
        data.add(new POJO(3, "First", 11, 102, 3000L, "One", 10200L));
        data.add(new POJO(4, "First_", 11, 106, 1000L, "One", 10300L));
        data.add(new POJO(5, "First", 11, 102, 2000L, "One", 10100L));
        data.add(new POJO(6, "Second_", 20, 200, 2000L, "Two", 10100L));
        data.add(new POJO(7, "Third", 31, 301, 2000L, "Three", 10200L)); // 2x
        data.add(new POJO(8, "Third_", 30, 300, 1000L, "Three", 10100L));
        return env.fromCollection(data);
    }

    /** POJO. */
    public static class POJO {
        public int number;
        public String str;
        public Tuple2<Integer, CustomType> nestedTupleWithCustom;
        public NestedPojo nestedPojo;
        public transient Long ignoreMe;

        public POJO(int i0, String s0, int i1, int i2, long l0, String s1, long l1) {
            this.number = i0;
            this.str = s0;
            this.nestedTupleWithCustom = new Tuple2<>(i1, new CustomType(i2, l0, s1));
            this.nestedPojo = new NestedPojo();
            this.nestedPojo.longNumber = l1;
        }

        public POJO() {}

        @Override
        public String toString() {
            return number + " " + str + " " + nestedTupleWithCustom + " " + nestedPojo.longNumber;
        }
    }

    /** Nested POJO. */
    public static class NestedPojo {
        public static Object ignoreMe;
        public long longNumber;

        public NestedPojo() {}
    }

    public static DataSet<CrazyNested> getCrazyNestedDataSet(ExecutionEnvironment env) {
        List<CrazyNested> data = new ArrayList<>();
        data.add(new CrazyNested("aa"));
        data.add(new CrazyNested("bb"));
        data.add(new CrazyNested("bb"));
        data.add(new CrazyNested("cc"));
        data.add(new CrazyNested("cc"));
        data.add(new CrazyNested("cc"));
        return env.fromCollection(data);
    }

    /** Deeply nested POJO. */
    public static class CrazyNested {
        public CrazyNestedL1 nestLvl1;
        public Long something; // test proper null-value handling

        public CrazyNested() {}

        public CrazyNested(
                String set,
                String second,
                long s) { // additional CTor to set all fields to non-null values
            this(set);
            something = s;
            nestLvl1.a = second;
        }

        public CrazyNested(String set) {
            nestLvl1 = new CrazyNestedL1();
            nestLvl1.nestLvl2 = new CrazyNestedL2();
            nestLvl1.nestLvl2.nestLvl3 = new CrazyNestedL3();
            nestLvl1.nestLvl2.nestLvl3.nestLvl4 = new CrazyNestedL4();
            nestLvl1.nestLvl2.nestLvl3.nestLvl4.f1nal = set;
        }
    }

    /** Nested POJO level 2. */
    public static class CrazyNestedL1 {
        public String a;
        public int b;
        public CrazyNestedL2 nestLvl2;
    }

    /** Nested POJO level 3. */
    public static class CrazyNestedL2 {
        public CrazyNestedL3 nestLvl3;
    }

    /** Nested POJO level 4. */
    public static class CrazyNestedL3 {
        public CrazyNestedL4 nestLvl4;
    }

    /** Nested POJO level 5. */
    public static class CrazyNestedL4 {
        public String f1nal;
    }

    // Copied from TypeExtractorTest
    private static class FromTuple extends Tuple3<String, String, Long> {
        private static final long serialVersionUID = 1L;
        public int special;
    }

    /** Pojo extending from tuple WITH custom fields. */
    public static class FromTupleWithCTor extends FromTuple {

        private static final long serialVersionUID = 1L;

        public FromTupleWithCTor() {}

        public FromTupleWithCTor(int special, long tupleField) {
            this.special = special;
            this.setField(tupleField, 2);
        }
    }

    public static DataSet<FromTupleWithCTor> getPojoExtendingFromTuple(ExecutionEnvironment env) {
        List<FromTupleWithCTor> data = new ArrayList<>();
        data.add(new FromTupleWithCTor(1, 10L)); // 3x
        data.add(new FromTupleWithCTor(1, 10L));
        data.add(new FromTupleWithCTor(1, 10L));
        data.add(new FromTupleWithCTor(2, 20L)); // 2x
        data.add(new FromTupleWithCTor(2, 20L));
        return env.fromCollection(data);
    }

    /** POJO with Tuple and Writable. */
    public static class PojoContainingTupleAndWritable {
        public int someInt;
        public String someString;
        public IntWritable hadoopFan;
        public Tuple2<Long, Long> theTuple;

        public PojoContainingTupleAndWritable() {}

        public PojoContainingTupleAndWritable(int i, long l1, long l2) {
            hadoopFan = new IntWritable(i);
            someInt = i;
            theTuple = new Tuple2<>(l1, l2);
        }
    }

    public static DataSet<PojoContainingTupleAndWritable> getPojoContainingTupleAndWritable(
            ExecutionEnvironment env) {
        List<PojoContainingTupleAndWritable> data = new ArrayList<>();
        data.add(new PojoContainingTupleAndWritable(1, 10L, 100L)); // 1x
        data.add(new PojoContainingTupleAndWritable(2, 20L, 200L)); // 5x
        data.add(new PojoContainingTupleAndWritable(2, 20L, 200L));
        data.add(new PojoContainingTupleAndWritable(2, 20L, 200L));
        data.add(new PojoContainingTupleAndWritable(2, 20L, 200L));
        data.add(new PojoContainingTupleAndWritable(2, 20L, 200L));
        return env.fromCollection(data);
    }

    public static DataSet<PojoContainingTupleAndWritable>
            getGroupSortedPojoContainingTupleAndWritable(ExecutionEnvironment env) {
        List<PojoContainingTupleAndWritable> data = new ArrayList<>();
        data.add(new PojoContainingTupleAndWritable(1, 10L, 100L)); // 1x
        data.add(new PojoContainingTupleAndWritable(2, 20L, 200L)); // 5x
        data.add(new PojoContainingTupleAndWritable(2, 20L, 201L));
        data.add(new PojoContainingTupleAndWritable(2, 30L, 200L));
        data.add(new PojoContainingTupleAndWritable(2, 30L, 600L));
        data.add(new PojoContainingTupleAndWritable(2, 30L, 400L));
        return env.fromCollection(data);
    }

    public static DataSet<Tuple3<Integer, CrazyNested, POJO>> getTupleContainingPojos(
            ExecutionEnvironment env) {
        List<Tuple3<Integer, CrazyNested, POJO>> data = new ArrayList<>();
        data.add(
                new Tuple3<>(
                        1,
                        new CrazyNested("one", "uno", 1L),
                        new POJO(1, "First", 10, 100, 1000L, "One", 10000L))); // 3x
        data.add(
                new Tuple3<>(
                        1,
                        new CrazyNested("one", "uno", 1L),
                        new POJO(1, "First", 10, 100, 1000L, "One", 10000L)));
        data.add(
                new Tuple3<>(
                        1,
                        new CrazyNested("one", "uno", 1L),
                        new POJO(1, "First", 10, 100, 1000L, "One", 10000L)));
        // POJO is not initialized according to the first two fields.
        data.add(
                new Tuple3<>(
                        2,
                        new CrazyNested("two", "duo", 2L),
                        new POJO(1, "First", 10, 100, 1000L, "One", 10000L))); // 1x
        return env.fromCollection(data);
    }

    /** POJO. */
    public static class Pojo1 {
        public String a;
        public String b;

        public Pojo1() {}

        public Pojo1(String a, String b) {
            this.a = a;
            this.b = b;
        }
    }

    /** Another POJO. */
    public static class Pojo2 {
        public String a2;
        public String b2;
    }

    /** Nested POJO. */
    public static class PojoWithMultiplePojos {
        public Pojo1 p1;
        public Pojo2 p2;
        public Integer i0;

        public PojoWithMultiplePojos() {}

        public PojoWithMultiplePojos(String a, String b, String a1, String b1, Integer i0) {
            p1 = new Pojo1();
            p1.a = a;
            p1.b = b;
            p2 = new Pojo2();
            p2.a2 = a1;
            p2.b2 = b1;
            this.i0 = i0;
        }
    }

    public static DataSet<PojoWithMultiplePojos> getPojoWithMultiplePojos(
            ExecutionEnvironment env) {
        List<PojoWithMultiplePojos> data = new ArrayList<>();
        data.add(new PojoWithMultiplePojos("a", "aa", "b", "bb", 1));
        data.add(new PojoWithMultiplePojos("b", "bb", "c", "cc", 2));
        data.add(new PojoWithMultiplePojos("b", "bb", "c", "cc", 2));
        data.add(new PojoWithMultiplePojos("b", "bb", "c", "cc", 2));
        data.add(new PojoWithMultiplePojos("d", "dd", "e", "ee", 3));
        data.add(new PojoWithMultiplePojos("d", "dd", "e", "ee", 3));
        return env.fromCollection(data);
    }

    /** Custom enum. */
    public enum Category {
        CAT_A,
        CAT_B
    }

    /** POJO with Date and enum. */
    public static class PojoWithDateAndEnum {
        public String group;
        public Date date;
        public Category cat;
    }

    public static DataSet<PojoWithDateAndEnum> getPojoWithDateAndEnum(ExecutionEnvironment env) {
        List<PojoWithDateAndEnum> data = new ArrayList<>();

        PojoWithDateAndEnum one = new PojoWithDateAndEnum();
        one.group = "a";
        one.date = new Date(666);
        one.cat = Category.CAT_A;
        data.add(one);

        PojoWithDateAndEnum two = new PojoWithDateAndEnum();
        two.group = "a";
        two.date = new Date(666);
        two.cat = Category.CAT_A;
        data.add(two);

        PojoWithDateAndEnum three = new PojoWithDateAndEnum();
        three.group = "b";
        three.date = new Date(666);
        three.cat = Category.CAT_B;
        data.add(three);

        return env.fromCollection(data);
    }

    /** POJO with collection. */
    public static class PojoWithCollection {
        public List<Pojo1> pojos;
        public int key;
        public java.sql.Date sqlDate;
        public BigInteger bigInt;
        public BigDecimal bigDecimalKeepItNull;
        public BigInt scalaBigInt;
        public List<Object> mixed;

        @Override
        public String toString() {
            return "PojoWithCollection{"
                    + "pojos.size()="
                    + pojos.size()
                    + ", key="
                    + key
                    + ", sqlDate="
                    + sqlDate
                    + ", bigInt="
                    + bigInt
                    + ", bigDecimalKeepItNull="
                    + bigDecimalKeepItNull
                    + ", scalaBigInt="
                    + scalaBigInt
                    + ", mixed="
                    + mixed
                    + '}';
        }
    }

    /** POJO with generic collection. */
    public static class PojoWithCollectionGeneric {
        public List<Pojo1> pojos;
        public int key;
        public java.sql.Date sqlDate;
        public BigInteger bigInt;
        public BigDecimal bigDecimalKeepItNull;
        public BigInt scalaBigInt;
        public List<Object> mixed;
        private PojoWithDateAndEnum makeMeGeneric;

        @Override
        public String toString() {
            return "PojoWithCollection{"
                    + "pojos.size()="
                    + pojos.size()
                    + ", key="
                    + key
                    + ", sqlDate="
                    + sqlDate
                    + ", bigInt="
                    + bigInt
                    + ", bigDecimalKeepItNull="
                    + bigDecimalKeepItNull
                    + ", scalaBigInt="
                    + scalaBigInt
                    + ", mixed="
                    + mixed
                    + '}';
        }
    }

    public static DataSet<PojoWithCollection> getPojoWithCollection(ExecutionEnvironment env) {
        List<PojoWithCollection> data = new ArrayList<>();

        List<Pojo1> pojosList1 = new ArrayList<>();
        pojosList1.add(new Pojo1("a", "aa"));
        pojosList1.add(new Pojo1("b", "bb"));

        List<Pojo1> pojosList2 = new ArrayList<>();
        pojosList2.add(new Pojo1("a2", "aa2"));
        pojosList2.add(new Pojo1("b2", "bb2"));

        PojoWithCollection pwc1 = new PojoWithCollection();
        pwc1.pojos = pojosList1;
        pwc1.key = 0;
        pwc1.bigInt = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.TEN);
        pwc1.scalaBigInt = BigInt.int2bigInt(10);
        pwc1.bigDecimalKeepItNull = null;

        // use calendar to make it stable across time zones
        GregorianCalendar gcl1 = new GregorianCalendar(2033, 4, 18);
        pwc1.sqlDate = new java.sql.Date(gcl1.getTimeInMillis());
        pwc1.mixed = new ArrayList<>();
        Map<String, Integer> map = new HashMap<>();
        map.put("someKey", 1); // map.put("anotherKey", 2); map.put("third", 3);
        pwc1.mixed.add(map);
        pwc1.mixed.add(new File("/this/is/wrong"));
        pwc1.mixed.add("uhlala");

        PojoWithCollection pwc2 = new PojoWithCollection();
        pwc2.pojos = pojosList2;
        pwc2.key = 0;
        pwc2.bigInt = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.TEN);
        pwc2.scalaBigInt = BigInt.int2bigInt(31104000);
        pwc2.bigDecimalKeepItNull = null;

        GregorianCalendar gcl2 = new GregorianCalendar(1976, 4, 3);
        pwc2.sqlDate = new java.sql.Date(gcl2.getTimeInMillis()); // 1976

        data.add(pwc1);
        data.add(pwc2);

        return env.fromCollection(data);
    }
}
