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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import scala.math.BigInt;

/**
 * #######################################################################################################
 * BE AWARE THAT OTHER TESTS DEPEND ON THIS TEST DATA. IF YOU MODIFY THE DATA MAKE SURE YOU CHECK
 * THAT ALL TESTS ARE STILL WORKING!
 * #######################################################################################################
 */
public class CollectionDataStreams {

    public static DataStreamSource<Tuple3<Integer, Long, String>> get3TupleDataSet(
            StreamExecutionEnvironment env) {

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

        return env.fromData(data);
    }

    public static DataStreamSource<Tuple3<Integer, Long, String>> getSmall3TupleDataSet(
            StreamExecutionEnvironment env) {

        List<Tuple3<Integer, Long, String>> data = new ArrayList<>();
        data.add(new Tuple3<>(1, 1L, "Hi"));
        data.add(new Tuple3<>(2, 2L, "Hello"));
        data.add(new Tuple3<>(3, 2L, "Hello world"));

        Collections.shuffle(data);

        return env.fromData(data);
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
}
