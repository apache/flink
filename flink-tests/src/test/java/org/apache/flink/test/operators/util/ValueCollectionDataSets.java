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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;

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
public class ValueCollectionDataSets {

    public static DataSet<Tuple3<IntValue, LongValue, StringValue>> get3TupleDataSet(
            ExecutionEnvironment env) {
        List<Tuple3<IntValue, LongValue, StringValue>> data = new ArrayList<>();

        data.add(new Tuple3<>(new IntValue(1), new LongValue(1L), new StringValue("Hi")));
        data.add(new Tuple3<>(new IntValue(2), new LongValue(2L), new StringValue("Hello")));
        data.add(new Tuple3<>(new IntValue(3), new LongValue(2L), new StringValue("Hello world")));
        data.add(
                new Tuple3<>(
                        new IntValue(4),
                        new LongValue(3L),
                        new StringValue("Hello world, how are you?")));
        data.add(new Tuple3<>(new IntValue(5), new LongValue(3L), new StringValue("I am fine.")));
        data.add(
                new Tuple3<>(
                        new IntValue(6), new LongValue(3L), new StringValue("Luke Skywalker")));
        data.add(new Tuple3<>(new IntValue(7), new LongValue(4L), new StringValue("Comment#1")));
        data.add(new Tuple3<>(new IntValue(8), new LongValue(4L), new StringValue("Comment#2")));
        data.add(new Tuple3<>(new IntValue(9), new LongValue(4L), new StringValue("Comment#3")));
        data.add(new Tuple3<>(new IntValue(10), new LongValue(4L), new StringValue("Comment#4")));
        data.add(new Tuple3<>(new IntValue(11), new LongValue(5L), new StringValue("Comment#5")));
        data.add(new Tuple3<>(new IntValue(12), new LongValue(5L), new StringValue("Comment#6")));
        data.add(new Tuple3<>(new IntValue(13), new LongValue(5L), new StringValue("Comment#7")));
        data.add(new Tuple3<>(new IntValue(14), new LongValue(5L), new StringValue("Comment#8")));
        data.add(new Tuple3<>(new IntValue(15), new LongValue(5L), new StringValue("Comment#9")));
        data.add(new Tuple3<>(new IntValue(16), new LongValue(6L), new StringValue("Comment#10")));
        data.add(new Tuple3<>(new IntValue(17), new LongValue(6L), new StringValue("Comment#11")));
        data.add(new Tuple3<>(new IntValue(18), new LongValue(6L), new StringValue("Comment#12")));
        data.add(new Tuple3<>(new IntValue(19), new LongValue(6L), new StringValue("Comment#13")));
        data.add(new Tuple3<>(new IntValue(20), new LongValue(6L), new StringValue("Comment#14")));
        data.add(new Tuple3<>(new IntValue(21), new LongValue(6L), new StringValue("Comment#15")));

        Collections.shuffle(data);

        return env.fromCollection(data);
    }

    public static DataSet<Tuple3<IntValue, LongValue, StringValue>> getSmall3TupleDataSet(
            ExecutionEnvironment env) {
        List<Tuple3<IntValue, LongValue, StringValue>> data = new ArrayList<>();

        data.add(new Tuple3<>(new IntValue(1), new LongValue(1L), new StringValue("Hi")));
        data.add(new Tuple3<>(new IntValue(2), new LongValue(2L), new StringValue("Hello")));
        data.add(new Tuple3<>(new IntValue(3), new LongValue(2L), new StringValue("Hello world")));

        Collections.shuffle(data);

        return env.fromCollection(data);
    }

    public static DataSet<Tuple5<IntValue, LongValue, IntValue, StringValue, LongValue>>
            get5TupleDataSet(ExecutionEnvironment env) {
        List<Tuple5<IntValue, LongValue, IntValue, StringValue, LongValue>> data =
                new ArrayList<>();

        data.add(
                new Tuple5<>(
                        new IntValue(1),
                        new LongValue(1L),
                        new IntValue(0),
                        new StringValue("Hallo"),
                        new LongValue(1L)));
        data.add(
                new Tuple5<>(
                        new IntValue(2),
                        new LongValue(2L),
                        new IntValue(1),
                        new StringValue("Hallo Welt"),
                        new LongValue(2L)));
        data.add(
                new Tuple5<>(
                        new IntValue(2),
                        new LongValue(3L),
                        new IntValue(2),
                        new StringValue("Hallo Welt wie"),
                        new LongValue(1L)));
        data.add(
                new Tuple5<>(
                        new IntValue(3),
                        new LongValue(4L),
                        new IntValue(3),
                        new StringValue("Hallo Welt wie gehts?"),
                        new LongValue(2L)));
        data.add(
                new Tuple5<>(
                        new IntValue(3),
                        new LongValue(5L),
                        new IntValue(4),
                        new StringValue("ABC"),
                        new LongValue(2L)));
        data.add(
                new Tuple5<>(
                        new IntValue(3),
                        new LongValue(6L),
                        new IntValue(5),
                        new StringValue("BCD"),
                        new LongValue(3L)));
        data.add(
                new Tuple5<>(
                        new IntValue(4),
                        new LongValue(7L),
                        new IntValue(6),
                        new StringValue("CDE"),
                        new LongValue(2L)));
        data.add(
                new Tuple5<>(
                        new IntValue(4),
                        new LongValue(8L),
                        new IntValue(7),
                        new StringValue("DEF"),
                        new LongValue(1L)));
        data.add(
                new Tuple5<>(
                        new IntValue(4),
                        new LongValue(9L),
                        new IntValue(8),
                        new StringValue("EFG"),
                        new LongValue(1L)));
        data.add(
                new Tuple5<>(
                        new IntValue(4),
                        new LongValue(10L),
                        new IntValue(9),
                        new StringValue("FGH"),
                        new LongValue(2L)));
        data.add(
                new Tuple5<>(
                        new IntValue(5),
                        new LongValue(11L),
                        new IntValue(10),
                        new StringValue("GHI"),
                        new LongValue(1L)));
        data.add(
                new Tuple5<>(
                        new IntValue(5),
                        new LongValue(12L),
                        new IntValue(11),
                        new StringValue("HIJ"),
                        new LongValue(3L)));
        data.add(
                new Tuple5<>(
                        new IntValue(5),
                        new LongValue(13L),
                        new IntValue(12),
                        new StringValue("IJK"),
                        new LongValue(3L)));
        data.add(
                new Tuple5<>(
                        new IntValue(5),
                        new LongValue(14L),
                        new IntValue(13),
                        new StringValue("JKL"),
                        new LongValue(2L)));
        data.add(
                new Tuple5<>(
                        new IntValue(5),
                        new LongValue(15L),
                        new IntValue(14),
                        new StringValue("KLM"),
                        new LongValue(2L)));

        Collections.shuffle(data);

        TupleTypeInfo<Tuple5<IntValue, LongValue, IntValue, StringValue, LongValue>> type =
                new TupleTypeInfo<>(
                        ValueTypeInfo.INT_VALUE_TYPE_INFO,
                        ValueTypeInfo.LONG_VALUE_TYPE_INFO,
                        ValueTypeInfo.INT_VALUE_TYPE_INFO,
                        ValueTypeInfo.STRING_VALUE_TYPE_INFO,
                        ValueTypeInfo.LONG_VALUE_TYPE_INFO);

        return env.fromCollection(data, type);
    }

    public static DataSet<Tuple5<IntValue, LongValue, IntValue, StringValue, LongValue>>
            getSmall5TupleDataSet(ExecutionEnvironment env) {
        List<Tuple5<IntValue, LongValue, IntValue, StringValue, LongValue>> data =
                new ArrayList<>();

        data.add(
                new Tuple5<>(
                        new IntValue(1),
                        new LongValue(1L),
                        new IntValue(0),
                        new StringValue("Hallo"),
                        new LongValue(1L)));
        data.add(
                new Tuple5<>(
                        new IntValue(2),
                        new LongValue(2L),
                        new IntValue(1),
                        new StringValue("Hallo Welt"),
                        new LongValue(2L)));
        data.add(
                new Tuple5<>(
                        new IntValue(2),
                        new LongValue(3L),
                        new IntValue(2),
                        new StringValue("Hallo Welt wie"),
                        new LongValue(1L)));

        Collections.shuffle(data);

        TupleTypeInfo<Tuple5<IntValue, LongValue, IntValue, StringValue, LongValue>> type =
                new TupleTypeInfo<>(
                        ValueTypeInfo.INT_VALUE_TYPE_INFO,
                        ValueTypeInfo.LONG_VALUE_TYPE_INFO,
                        ValueTypeInfo.INT_VALUE_TYPE_INFO,
                        ValueTypeInfo.STRING_VALUE_TYPE_INFO,
                        ValueTypeInfo.LONG_VALUE_TYPE_INFO);

        return env.fromCollection(data, type);
    }

    public static DataSet<Tuple2<Tuple2<IntValue, IntValue>, StringValue>>
            getSmallNestedTupleDataSet(ExecutionEnvironment env) {
        List<Tuple2<Tuple2<IntValue, IntValue>, StringValue>> data = new ArrayList<>();

        data.add(
                new Tuple2<>(
                        new Tuple2<>(new IntValue(1), new IntValue(1)), new StringValue("one")));
        data.add(
                new Tuple2<>(
                        new Tuple2<>(new IntValue(2), new IntValue(2)), new StringValue("two")));
        data.add(
                new Tuple2<>(
                        new Tuple2<>(new IntValue(3), new IntValue(3)), new StringValue("three")));

        TupleTypeInfo<Tuple2<Tuple2<IntValue, IntValue>, StringValue>> type =
                new TupleTypeInfo<>(
                        new TupleTypeInfo<Tuple2<IntValue, IntValue>>(
                                ValueTypeInfo.INT_VALUE_TYPE_INFO,
                                ValueTypeInfo.INT_VALUE_TYPE_INFO),
                        ValueTypeInfo.STRING_VALUE_TYPE_INFO);

        return env.fromCollection(data, type);
    }

    public static DataSet<Tuple2<Tuple2<IntValue, IntValue>, StringValue>>
            getGroupSortedNestedTupleDataSet(ExecutionEnvironment env) {
        List<Tuple2<Tuple2<IntValue, IntValue>, StringValue>> data = new ArrayList<>();

        data.add(
                new Tuple2<>(new Tuple2<>(new IntValue(1), new IntValue(3)), new StringValue("a")));
        data.add(
                new Tuple2<>(new Tuple2<>(new IntValue(1), new IntValue(2)), new StringValue("a")));
        data.add(
                new Tuple2<>(new Tuple2<>(new IntValue(2), new IntValue(1)), new StringValue("a")));
        data.add(
                new Tuple2<>(new Tuple2<>(new IntValue(2), new IntValue(2)), new StringValue("b")));
        data.add(
                new Tuple2<>(new Tuple2<>(new IntValue(3), new IntValue(3)), new StringValue("c")));
        data.add(
                new Tuple2<>(new Tuple2<>(new IntValue(3), new IntValue(6)), new StringValue("c")));
        data.add(
                new Tuple2<>(new Tuple2<>(new IntValue(4), new IntValue(9)), new StringValue("c")));

        TupleTypeInfo<Tuple2<Tuple2<IntValue, IntValue>, StringValue>> type =
                new TupleTypeInfo<>(
                        new TupleTypeInfo<Tuple2<IntValue, IntValue>>(
                                ValueTypeInfo.INT_VALUE_TYPE_INFO,
                                ValueTypeInfo.INT_VALUE_TYPE_INFO),
                        ValueTypeInfo.STRING_VALUE_TYPE_INFO);

        return env.fromCollection(data, type);
    }

    public static DataSet<Tuple3<Tuple2<IntValue, IntValue>, StringValue, IntValue>>
            getGroupSortedNestedTupleDataSet2(ExecutionEnvironment env) {
        List<Tuple3<Tuple2<IntValue, IntValue>, StringValue, IntValue>> data = new ArrayList<>();

        data.add(
                new Tuple3<>(
                        new Tuple2<IntValue, IntValue>(new IntValue(1), new IntValue(3)),
                        new StringValue("a"),
                        new IntValue(2)));
        data.add(
                new Tuple3<>(
                        new Tuple2<IntValue, IntValue>(new IntValue(1), new IntValue(2)),
                        new StringValue("a"),
                        new IntValue(1)));
        data.add(
                new Tuple3<>(
                        new Tuple2<IntValue, IntValue>(new IntValue(2), new IntValue(1)),
                        new StringValue("a"),
                        new IntValue(3)));
        data.add(
                new Tuple3<>(
                        new Tuple2<IntValue, IntValue>(new IntValue(2), new IntValue(2)),
                        new StringValue("b"),
                        new IntValue(4)));
        data.add(
                new Tuple3<>(
                        new Tuple2<IntValue, IntValue>(new IntValue(3), new IntValue(3)),
                        new StringValue("c"),
                        new IntValue(5)));
        data.add(
                new Tuple3<>(
                        new Tuple2<IntValue, IntValue>(new IntValue(3), new IntValue(6)),
                        new StringValue("c"),
                        new IntValue(6)));
        data.add(
                new Tuple3<>(
                        new Tuple2<IntValue, IntValue>(new IntValue(4), new IntValue(9)),
                        new StringValue("c"),
                        new IntValue(7)));

        TupleTypeInfo<Tuple3<Tuple2<IntValue, IntValue>, StringValue, IntValue>> type =
                new TupleTypeInfo<>(
                        new TupleTypeInfo<Tuple2<IntValue, IntValue>>(
                                ValueTypeInfo.INT_VALUE_TYPE_INFO,
                                ValueTypeInfo.INT_VALUE_TYPE_INFO),
                        ValueTypeInfo.STRING_VALUE_TYPE_INFO,
                        ValueTypeInfo.INT_VALUE_TYPE_INFO);

        return env.fromCollection(data, type);
    }

    public static DataSet<StringValue> getStringDataSet(ExecutionEnvironment env) {
        List<StringValue> data = new ArrayList<>();

        data.add(new StringValue("Hi"));
        data.add(new StringValue("Hello"));
        data.add(new StringValue("Hello world"));
        data.add(new StringValue("Hello world, how are you?"));
        data.add(new StringValue("I am fine."));
        data.add(new StringValue("Luke Skywalker"));
        data.add(new StringValue("Random comment"));
        data.add(new StringValue("LOL"));

        Collections.shuffle(data);

        return env.fromCollection(data);
    }

    public static DataSet<IntValue> getIntDataSet(ExecutionEnvironment env) {
        List<IntValue> data = new ArrayList<>();

        data.add(new IntValue(1));
        data.add(new IntValue(2));
        data.add(new IntValue(2));
        data.add(new IntValue(3));
        data.add(new IntValue(3));
        data.add(new IntValue(3));
        data.add(new IntValue(4));
        data.add(new IntValue(4));
        data.add(new IntValue(4));
        data.add(new IntValue(4));
        data.add(new IntValue(5));
        data.add(new IntValue(5));
        data.add(new IntValue(5));
        data.add(new IntValue(5));
        data.add(new IntValue(5));

        Collections.shuffle(data);

        return env.fromCollection(data);
    }

    public static DataSet<CustomType> getCustomTypeDataSet(ExecutionEnvironment env) {
        List<CustomType> data = new ArrayList<CustomType>();

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
        List<CustomType> data = new ArrayList<CustomType>();

        data.add(new CustomType(1, 0L, "Hi"));
        data.add(new CustomType(2, 1L, "Hello"));
        data.add(new CustomType(2, 2L, "Hello world"));

        Collections.shuffle(data);

        return env.fromCollection(data);
    }

    /** POJO. */
    public static class CustomType implements Serializable {

        private static final long serialVersionUID = 1L;

        public IntValue myInt;
        public LongValue myLong;
        public StringValue myString;

        public CustomType() {}

        public CustomType(int i, long l, String s) {
            myInt = new IntValue(i);
            myLong = new LongValue(l);
            myString = new StringValue(s);
        }

        @Override
        public String toString() {
            return myInt + "," + myLong + "," + myString;
        }
    }

    private static class CustomTypeComparator implements Comparator<CustomType> {

        @Override
        public int compare(CustomType o1, CustomType o2) {
            int diff = o1.myInt.getValue() - o2.myInt.getValue();
            if (diff != 0) {
                return diff;
            }
            diff = (int) (o1.myLong.getValue() - o2.myLong.getValue());
            return diff != 0 ? diff : o1.myString.getValue().compareTo(o2.myString.getValue());
        }
    }

    private static DataSet<
                    Tuple7<
                            IntValue,
                            StringValue,
                            IntValue,
                            IntValue,
                            LongValue,
                            StringValue,
                            LongValue>>
            getSmallTuplebasedDataSet(ExecutionEnvironment env) {
        List<Tuple7<IntValue, StringValue, IntValue, IntValue, LongValue, StringValue, LongValue>>
                data = new ArrayList<>();

        data.add(
                new Tuple7<>(
                        new IntValue(1),
                        new StringValue("First"),
                        new IntValue(10),
                        new IntValue(100),
                        new LongValue(1000L),
                        new StringValue("One"),
                        new LongValue(10000L)));
        data.add(
                new Tuple7<>(
                        new IntValue(2),
                        new StringValue("Second"),
                        new IntValue(20),
                        new IntValue(200),
                        new LongValue(2000L),
                        new StringValue("Two"),
                        new LongValue(20000L)));
        data.add(
                new Tuple7<>(
                        new IntValue(3),
                        new StringValue("Third"),
                        new IntValue(30),
                        new IntValue(300),
                        new LongValue(3000L),
                        new StringValue("Three"),
                        new LongValue(30000L)));

        return env.fromCollection(data);
    }

    private static DataSet<
                    Tuple7<
                            LongValue,
                            IntValue,
                            IntValue,
                            LongValue,
                            StringValue,
                            IntValue,
                            StringValue>>
            getSmallTuplebasedDataSetMatchingPojo(ExecutionEnvironment env) {
        List<Tuple7<LongValue, IntValue, IntValue, LongValue, StringValue, IntValue, StringValue>>
                data = new ArrayList<>();

        data.add(
                new Tuple7<>(
                        new LongValue(10000L),
                        new IntValue(10),
                        new IntValue(100),
                        new LongValue(1000L),
                        new StringValue("One"),
                        new IntValue(1),
                        new StringValue("First")));
        data.add(
                new Tuple7<>(
                        new LongValue(20000L),
                        new IntValue(20),
                        new IntValue(200),
                        new LongValue(2000L),
                        new StringValue("Two"),
                        new IntValue(2),
                        new StringValue("Second")));
        data.add(
                new Tuple7<>(
                        new LongValue(30000L),
                        new IntValue(30),
                        new IntValue(300),
                        new LongValue(3000L),
                        new StringValue("Three"),
                        new IntValue(3),
                        new StringValue("Third")));

        return env.fromCollection(data);
    }

    private static DataSet<POJO> getSmallPojoDataSet(ExecutionEnvironment env) {
        List<POJO> data = new ArrayList<POJO>();

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

    private static DataSet<POJO> getDuplicatePojoDataSet(ExecutionEnvironment env) {
        List<POJO> data = new ArrayList<POJO>();

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

    private static DataSet<POJO> getMixedPojoDataSet(ExecutionEnvironment env) {
        List<POJO> data = new ArrayList<POJO>();

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
        public IntValue number;
        public StringValue str;
        public Tuple2<IntValue, CustomType> nestedTupleWithCustom;
        public NestedPojo nestedPojo;
        public transient LongValue ignoreMe;

        public POJO(int i0, String s0, int i1, int i2, long l0, String s1, long l1) {
            this.number = new IntValue(i0);
            this.str = new StringValue(s0);
            this.nestedTupleWithCustom = new Tuple2<>(new IntValue(i1), new CustomType(i2, l0, s1));
            this.nestedPojo = new NestedPojo();
            this.nestedPojo.longNumber = new LongValue(l1);
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
        public LongValue longNumber;

        public NestedPojo() {}
    }

    private static DataSet<CrazyNested> getCrazyNestedDataSet(ExecutionEnvironment env) {
        List<CrazyNested> data = new ArrayList<CrazyNested>();

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
        public LongValue something; // test proper null-value handling

        public CrazyNested() {}

        public CrazyNested(
                String set,
                String second,
                long s) { // additional CTor to set all fields to non-null values
            this(set);
            something = new LongValue(s);
            nestLvl1.a = new StringValue(second);
        }

        public CrazyNested(String set) {
            nestLvl1 = new CrazyNestedL1();
            nestLvl1.nestLvl2 = new CrazyNestedL2();
            nestLvl1.nestLvl2.nestLvl3 = new CrazyNestedL3();
            nestLvl1.nestLvl2.nestLvl3.nestLvl4 = new CrazyNestedL4();
            nestLvl1.nestLvl2.nestLvl3.nestLvl4.f1nal = new StringValue(set);
        }
    }

    /** Nested POJO level 2. */
    public static class CrazyNestedL1 {
        public StringValue a;
        public IntValue b;
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
        public StringValue f1nal;
    }

    // Copied from TypeExtractorTest
    private static class FromTuple extends Tuple3<StringValue, StringValue, LongValue> {
        private static final long serialVersionUID = 1L;
        public IntValue special;
    }

    private static class FromTupleWithCTor extends FromTuple {

        private static final long serialVersionUID = 1L;

        public FromTupleWithCTor() {}

        public FromTupleWithCTor(int special, long tupleField) {
            this.special = new IntValue(special);
            this.setField(new LongValue(tupleField), 2);
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
        public IntValue someInt;
        public StringValue someString;
        public IntWritable hadoopFan;
        public Tuple2<LongValue, LongValue> theTuple;

        public PojoContainingTupleAndWritable() {}

        public PojoContainingTupleAndWritable(int i, long l1, long l2) {
            hadoopFan = new IntWritable(i);
            someInt = new IntValue(i);
            theTuple = new Tuple2<>(new LongValue(l1), new LongValue(l2));
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

    public static DataSet<Tuple3<IntValue, CrazyNested, POJO>> getTupleContainingPojos(
            ExecutionEnvironment env) {
        List<Tuple3<IntValue, CrazyNested, POJO>> data = new ArrayList<>();
        data.add(
                new Tuple3<IntValue, CrazyNested, POJO>(
                        new IntValue(1),
                        new CrazyNested("one", "uno", 1L),
                        new POJO(1, "First", 10, 100, 1000L, "One", 10000L))); // 3x
        data.add(
                new Tuple3<IntValue, CrazyNested, POJO>(
                        new IntValue(1),
                        new CrazyNested("one", "uno", 1L),
                        new POJO(1, "First", 10, 100, 1000L, "One", 10000L)));
        data.add(
                new Tuple3<IntValue, CrazyNested, POJO>(
                        new IntValue(1),
                        new CrazyNested("one", "uno", 1L),
                        new POJO(1, "First", 10, 100, 1000L, "One", 10000L)));
        // POJO is not initialized according to the first two fields.
        data.add(
                new Tuple3<IntValue, CrazyNested, POJO>(
                        new IntValue(2),
                        new CrazyNested("two", "duo", 2L),
                        new POJO(1, "First", 10, 100, 1000L, "One", 10000L))); // 1x
        return env.fromCollection(data);
    }

    /** POJO. */
    public static class Pojo1 {
        public StringValue a;
        public StringValue b;

        public Pojo1() {}

        public Pojo1(String a, String b) {
            this.a = new StringValue(a);
            this.b = new StringValue(b);
        }
    }

    /** POJO. */
    public static class Pojo2 {
        public StringValue a2;
        public StringValue b2;
    }

    /** Nested POJO. */
    public static class PojoWithMultiplePojos {
        public Pojo1 p1;
        public Pojo2 p2;
        public IntValue i0;

        public PojoWithMultiplePojos() {}

        public PojoWithMultiplePojos(String a, String b, String a1, String b1, int i0) {
            p1 = new Pojo1();
            p1.a = new StringValue(a);
            p1.b = new StringValue(b);
            p2 = new Pojo2();
            p2.a2 = new StringValue(a1);
            p2.b2 = new StringValue(b1);
            this.i0 = new IntValue(i0);
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
        CAT_B;
    }

    /** POJO with Data and enum. */
    public static class PojoWithDateAndEnum {
        public StringValue group;
        public Date date;
        public Category cat;
    }

    public static DataSet<PojoWithDateAndEnum> getPojoWithDateAndEnum(ExecutionEnvironment env) {
        List<PojoWithDateAndEnum> data = new ArrayList<PojoWithDateAndEnum>();

        PojoWithDateAndEnum one = new PojoWithDateAndEnum();
        one.group = new StringValue("a");
        one.date = new Date(666);
        one.cat = Category.CAT_A;
        data.add(one);

        PojoWithDateAndEnum two = new PojoWithDateAndEnum();
        two.group = new StringValue("a");
        two.date = new Date(666);
        two.cat = Category.CAT_A;
        data.add(two);

        PojoWithDateAndEnum three = new PojoWithDateAndEnum();
        three.group = new StringValue("b");
        three.date = new Date(666);
        three.cat = Category.CAT_B;
        data.add(three);

        return env.fromCollection(data);
    }

    /** POJO with collection. */
    public static class PojoWithCollection {
        public List<Pojo1> pojos;
        public IntValue key;
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
        public IntValue key;
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
        pwc1.key = new IntValue(0);
        pwc1.bigInt = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.TEN);
        pwc1.scalaBigInt = BigInt.int2bigInt(10);
        pwc1.bigDecimalKeepItNull = null;

        // use calendar to make it stable across time zones
        GregorianCalendar gcl1 = new GregorianCalendar(2033, 04, 18);
        pwc1.sqlDate = new java.sql.Date(gcl1.getTimeInMillis());
        pwc1.mixed = new ArrayList<Object>();
        Map<StringValue, IntValue> map = new HashMap<>();
        map.put(new StringValue("someKey"), new IntValue(1));
        pwc1.mixed.add(map);
        pwc1.mixed.add(new File("/this/is/wrong"));
        pwc1.mixed.add("uhlala");

        PojoWithCollection pwc2 = new PojoWithCollection();
        pwc2.pojos = pojosList2;
        pwc2.key = new IntValue(0);
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
