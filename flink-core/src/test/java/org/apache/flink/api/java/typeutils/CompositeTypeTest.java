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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CompositeTypeTest {

    private final TupleTypeInfo<?> tupleTypeInfo =
            new TupleTypeInfo<Tuple4<Integer, Integer, Integer, Integer>>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO);

    private final TupleTypeInfo<Tuple3<Integer, String, Long>> inNestedTuple1 =
            new TupleTypeInfo<Tuple3<Integer, String, Long>>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO);

    private final TupleTypeInfo<Tuple2<Double, Double>> inNestedTuple2 =
            new TupleTypeInfo<Tuple2<Double, Double>>(
                    BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO);

    private final TupleTypeInfo<?> nestedTypeInfo =
            new TupleTypeInfo<
                    Tuple4<
                            Integer,
                            Tuple3<Integer, String, Long>,
                            Integer,
                            Tuple2<Double, Double>>>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    inNestedTuple1,
                    BasicTypeInfo.INT_TYPE_INFO,
                    inNestedTuple2);

    private final TupleTypeInfo<Tuple2<Integer, Tuple2<Integer, Integer>>> inNestedTuple3 =
            new TupleTypeInfo<Tuple2<Integer, Tuple2<Integer, Integer>>>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    new TupleTypeInfo<Tuple2<Integer, Integer>>(
                            BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO));

    private final TupleTypeInfo<?> deepNestedTupleTypeInfo =
            new TupleTypeInfo<Tuple3<Integer, Tuple2<Integer, Tuple2<Integer, Integer>>, Integer>>(
                    BasicTypeInfo.INT_TYPE_INFO, inNestedTuple3, BasicTypeInfo.INT_TYPE_INFO);

    private final PojoTypeInfo<?> pojoTypeInfo =
            ((PojoTypeInfo<?>) TypeExtractor.getForClass(MyPojo.class));

    private final TupleTypeInfo<?> pojoInTupleTypeInfo =
            new TupleTypeInfo<Tuple2<Integer, MyPojo>>(BasicTypeInfo.INT_TYPE_INFO, pojoTypeInfo);

    @Test
    void testGetFlatFields() {
        assertThat(tupleTypeInfo.getFlatFields("0").get(0).getPosition()).isZero();
        assertThat(tupleTypeInfo.getFlatFields("1").get(0).getPosition()).isOne();
        assertThat(tupleTypeInfo.getFlatFields("2").get(0).getPosition()).isEqualTo(2);
        assertThat(tupleTypeInfo.getFlatFields("3").get(0).getPosition()).isEqualTo(3);
        assertThat(tupleTypeInfo.getFlatFields("f0").get(0).getPosition()).isZero();
        assertThat(tupleTypeInfo.getFlatFields("f1").get(0).getPosition()).isOne();
        assertThat(tupleTypeInfo.getFlatFields("f2").get(0).getPosition()).isEqualTo(2);
        assertThat(tupleTypeInfo.getFlatFields("f3").get(0).getPosition()).isEqualTo(3);

        assertThat(nestedTypeInfo.getFlatFields("0").get(0).getPosition()).isZero();
        assertThat(nestedTypeInfo.getFlatFields("1.0").get(0).getPosition()).isOne();
        assertThat(nestedTypeInfo.getFlatFields("1.1").get(0).getPosition()).isEqualTo(2);
        assertThat(nestedTypeInfo.getFlatFields("1.2").get(0).getPosition()).isEqualTo(3);
        assertThat(nestedTypeInfo.getFlatFields("2").get(0).getPosition()).isEqualTo(4);
        assertThat(nestedTypeInfo.getFlatFields("3.0").get(0).getPosition()).isEqualTo(5);
        assertThat(nestedTypeInfo.getFlatFields("3.1").get(0).getPosition()).isEqualTo(6);
        assertThat(nestedTypeInfo.getFlatFields("f2").get(0).getPosition()).isEqualTo(4);
        assertThat(nestedTypeInfo.getFlatFields("f3.f0").get(0).getPosition()).isEqualTo(5);
        assertThat(nestedTypeInfo.getFlatFields("1")).hasSize(3);
        assertThat(nestedTypeInfo.getFlatFields("1").get(0).getPosition()).isOne();
        assertThat(nestedTypeInfo.getFlatFields("1").get(1).getPosition()).isEqualTo(2);
        assertThat(nestedTypeInfo.getFlatFields("1").get(2).getPosition()).isEqualTo(3);
        assertThat(nestedTypeInfo.getFlatFields("1.*")).hasSize(3);
        assertThat(nestedTypeInfo.getFlatFields("1.*").get(0).getPosition()).isOne();
        assertThat(nestedTypeInfo.getFlatFields("1.*").get(1).getPosition()).isEqualTo(2);
        assertThat(nestedTypeInfo.getFlatFields("1.*").get(2).getPosition()).isEqualTo(3);
        assertThat(nestedTypeInfo.getFlatFields("3")).hasSize(2);
        assertThat(nestedTypeInfo.getFlatFields("3").get(0).getPosition()).isEqualTo(5);
        assertThat(nestedTypeInfo.getFlatFields("3").get(1).getPosition()).isEqualTo(6);
        assertThat(nestedTypeInfo.getFlatFields("f1")).hasSize(3);
        assertThat(nestedTypeInfo.getFlatFields("f1").get(0).getPosition()).isOne();
        assertThat(nestedTypeInfo.getFlatFields("f1").get(1).getPosition()).isEqualTo(2);
        assertThat(nestedTypeInfo.getFlatFields("f1").get(2).getPosition()).isEqualTo(3);
        assertThat(nestedTypeInfo.getFlatFields("f3")).hasSize(2);
        assertThat(nestedTypeInfo.getFlatFields("f3").get(0).getPosition()).isEqualTo(5);
        assertThat(nestedTypeInfo.getFlatFields("f3").get(1).getPosition()).isEqualTo(6);
        assertThat(nestedTypeInfo.getFlatFields("0").get(0).getType())
                .isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
        assertThat(nestedTypeInfo.getFlatFields("1.1").get(0).getType())
                .isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
        assertThat(nestedTypeInfo.getFlatFields("1").get(2).getType())
                .isEqualTo(BasicTypeInfo.LONG_TYPE_INFO);
        assertThat(nestedTypeInfo.getFlatFields("3").get(1).getType())
                .isEqualTo(BasicTypeInfo.DOUBLE_TYPE_INFO);

        assertThat(deepNestedTupleTypeInfo.getFlatFields("1")).hasSize(3);
        assertThat(deepNestedTupleTypeInfo.getFlatFields("1").get(0).getPosition()).isOne();
        assertThat(deepNestedTupleTypeInfo.getFlatFields("1").get(1).getPosition()).isEqualTo(2);
        assertThat(deepNestedTupleTypeInfo.getFlatFields("1").get(2).getPosition()).isEqualTo(3);
        assertThat(deepNestedTupleTypeInfo.getFlatFields("*")).hasSize(5);
        assertThat(deepNestedTupleTypeInfo.getFlatFields("*").get(0).getPosition()).isZero();
        assertThat(deepNestedTupleTypeInfo.getFlatFields("*").get(1).getPosition()).isOne();
        assertThat(deepNestedTupleTypeInfo.getFlatFields("*").get(2).getPosition()).isEqualTo(2);
        assertThat(deepNestedTupleTypeInfo.getFlatFields("*").get(3).getPosition()).isEqualTo(3);
        assertThat(deepNestedTupleTypeInfo.getFlatFields("*").get(4).getPosition()).isEqualTo(4);

        assertThat(pojoTypeInfo.getFlatFields("a").get(0).getPosition()).isZero();
        assertThat(pojoTypeInfo.getFlatFields("b").get(0).getPosition()).isOne();
        assertThat(pojoTypeInfo.getFlatFields("*")).hasSize(2);
        assertThat(pojoTypeInfo.getFlatFields("*").get(0).getPosition()).isZero();
        assertThat(pojoTypeInfo.getFlatFields("*").get(1).getPosition()).isOne();

        assertThat(pojoInTupleTypeInfo.getFlatFields("f1.a").get(0).getPosition()).isOne();
        assertThat(pojoInTupleTypeInfo.getFlatFields("1.b").get(0).getPosition()).isEqualTo(2);
        assertThat(pojoInTupleTypeInfo.getFlatFields("1")).hasSize(2);
        assertThat(pojoInTupleTypeInfo.getFlatFields("1.*").get(0).getPosition()).isOne();
        assertThat(pojoInTupleTypeInfo.getFlatFields("1").get(1).getPosition()).isEqualTo(2);
        assertThat(pojoInTupleTypeInfo.getFlatFields("f1.*")).hasSize(2);
        assertThat(pojoInTupleTypeInfo.getFlatFields("f1.*").get(0).getPosition()).isOne();
        assertThat(pojoInTupleTypeInfo.getFlatFields("f1").get(1).getPosition()).isEqualTo(2);
        assertThat(pojoInTupleTypeInfo.getFlatFields("*")).hasSize(3);
        assertThat(pojoInTupleTypeInfo.getFlatFields("*").get(0).getPosition()).isZero();
        assertThat(pojoInTupleTypeInfo.getFlatFields("*").get(1).getPosition()).isOne();
        assertThat(pojoInTupleTypeInfo.getFlatFields("*").get(2).getPosition()).isEqualTo(2);
    }

    @Test
    void testFieldAtStringRef() {

        assertThat(tupleTypeInfo.getTypeAt("0")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
        assertThat(tupleTypeInfo.getTypeAt("2")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
        assertThat(tupleTypeInfo.getTypeAt("f1")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
        assertThat(tupleTypeInfo.getTypeAt("f3")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);

        assertThat(nestedTypeInfo.getTypeAt("0")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
        assertThat(nestedTypeInfo.getTypeAt("1.0")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
        assertThat(nestedTypeInfo.getTypeAt("1.1")).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
        assertThat(nestedTypeInfo.getTypeAt("1.2")).isEqualTo(BasicTypeInfo.LONG_TYPE_INFO);
        assertThat(nestedTypeInfo.getTypeAt("2")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
        assertThat(nestedTypeInfo.getTypeAt("3.0")).isEqualTo(BasicTypeInfo.DOUBLE_TYPE_INFO);
        assertThat(nestedTypeInfo.getTypeAt("3.1")).isEqualTo(BasicTypeInfo.DOUBLE_TYPE_INFO);
        assertThat(nestedTypeInfo.getTypeAt("f2")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
        assertThat(nestedTypeInfo.getTypeAt("f3.f0")).isEqualTo(BasicTypeInfo.DOUBLE_TYPE_INFO);
        assertThat(nestedTypeInfo.getTypeAt("1")).isEqualTo(inNestedTuple1);
        assertThat(nestedTypeInfo.getTypeAt("3")).isEqualTo(inNestedTuple2);
        assertThat(nestedTypeInfo.getTypeAt("f1")).isEqualTo(inNestedTuple1);
        assertThat(nestedTypeInfo.getTypeAt("f3")).isEqualTo(inNestedTuple2);

        assertThat(deepNestedTupleTypeInfo.getTypeAt("1")).isEqualTo(inNestedTuple3);

        assertThat(pojoTypeInfo.getTypeAt("a")).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
        assertThat(pojoTypeInfo.getTypeAt("b")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);

        assertThat(pojoInTupleTypeInfo.getTypeAt("f1.a")).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
        assertThat(pojoInTupleTypeInfo.getTypeAt("1.b")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
        assertThat(pojoInTupleTypeInfo.getTypeAt("1")).isEqualTo(pojoTypeInfo);
        assertThat(pojoInTupleTypeInfo.getTypeAt("f1")).isEqualTo(pojoTypeInfo);
    }

    public static class MyPojo {
        public String a;
        public int b;
    }
}
