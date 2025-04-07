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

package org.apache.flink.streaming.util.typeutils;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for field accessors. */
class FieldAccessorTest {

    // Note, that AggregationFunctionTest indirectly also tests FieldAccessors.
    // ProductFieldAccessors are tested in CaseClassFieldAccessorTest.

    @Test
    void testFlatTuple() {
        Tuple2<String, Integer> t = Tuple2.of("aa", 5);
        TupleTypeInfo<Tuple2<String, Integer>> tpeInfo =
                (TupleTypeInfo<Tuple2<String, Integer>>) TypeExtractor.getForObject(t);

        FieldAccessor<Tuple2<String, Integer>, String> f0 =
                FieldAccessorFactory.getAccessor(tpeInfo, "f0", null);
        assertThat(f0.getFieldType().getTypeClass()).isEqualTo(String.class);
        assertThat(f0.get(t)).isEqualTo("aa");
        assertThat(t.f0).isEqualTo("aa");
        t = f0.set(t, "b");
        assertThat(f0.get(t)).isEqualTo("b");
        assertThat(t.f0).isEqualTo("b");

        FieldAccessor<Tuple2<String, Integer>, Integer> f1 =
                FieldAccessorFactory.getAccessor(tpeInfo, "f1", null);
        assertThat(f1.getFieldType().getTypeClass()).isEqualTo(Integer.class);
        assertThat(f1.get(t)).isEqualTo(5);
        assertThat(t.f1).isEqualTo(5);
        t = f1.set(t, 7);
        assertThat(f1.get(t)).isEqualTo(7);
        assertThat(t.f1).isEqualTo(7);
        assertThat(f0.get(t)).isEqualTo("b");
        assertThat(t.f0).isEqualTo("b");

        FieldAccessor<Tuple2<String, Integer>, Integer> f1n =
                FieldAccessorFactory.getAccessor(tpeInfo, 1, null);
        assertThat(f1n.getFieldType().getTypeClass()).isEqualTo(Integer.class);
        assertThat(f1n.get(t)).isEqualTo(7);
        assertThat(t.f1).isEqualTo(7);
        t = f1n.set(t, 10);
        assertThat(f1n.get(t)).isEqualTo(10);
        assertThat(f1.get(t)).isEqualTo(10);
        assertThat(t.f1).isEqualTo(10);
        assertThat(f0.get(t)).isEqualTo("b");
        assertThat(t.f0).isEqualTo("b");

        FieldAccessor<Tuple2<String, Integer>, Integer> f1ns =
                FieldAccessorFactory.getAccessor(tpeInfo, "1", null);
        assertThat(f1ns.getFieldType().getTypeClass()).isEqualTo(Integer.class);
        assertThat(f1ns.get(t)).isEqualTo(10);
        assertThat(t.f1).isEqualTo(10);
        t = f1ns.set(t, 11);
        assertThat(f1ns.get(t)).isEqualTo(11);
        assertThat(f1.get(t)).isEqualTo(11);
        assertThat(t.f1).isEqualTo(11);
        assertThat(f0.get(t)).isEqualTo("b");
        assertThat(t.f0).isEqualTo("b");

        // This is technically valid (the ".0" is selecting the 0th field of a basic type).
        FieldAccessor<Tuple2<String, Integer>, String> f0f0 =
                FieldAccessorFactory.getAccessor(tpeInfo, "f0.0", null);
        assertThat(f0f0.getFieldType().getTypeClass()).isEqualTo(String.class);
        assertThat(f0f0.get(t)).isEqualTo("b");
        assertThat(t.f0).isEqualTo("b");
        t = f0f0.set(t, "cc");
        assertThat(f0f0.get(t)).isEqualTo("cc");
        assertThat(t.f0).isEqualTo("cc");
    }

    @Test
    void testIllegalFlatTuple() {
        Tuple2<String, Integer> t = Tuple2.of("aa", 5);
        TupleTypeInfo<Tuple2<String, Integer>> tpeInfo =
                (TupleTypeInfo<Tuple2<String, Integer>>) TypeExtractor.getForObject(t);

        assertThatThrownBy(() -> FieldAccessorFactory.getAccessor(tpeInfo, "illegal", null))
                .isInstanceOf(CompositeType.InvalidFieldReferenceException.class);
    }

    @Test
    void testTupleInTuple() {
        Tuple2<String, Tuple3<Integer, Long, Double>> t = Tuple2.of("aa", Tuple3.of(5, 9L, 2.0));
        TupleTypeInfo<Tuple2<String, Tuple3<Integer, Long, Double>>> tpeInfo =
                (TupleTypeInfo<Tuple2<String, Tuple3<Integer, Long, Double>>>)
                        TypeExtractor.getForObject(t);

        FieldAccessor<Tuple2<String, Tuple3<Integer, Long, Double>>, String> f0 =
                FieldAccessorFactory.getAccessor(tpeInfo, "f0", null);
        assertThat(f0.getFieldType().getTypeClass()).isEqualTo(String.class);
        assertThat(f0.get(t)).isEqualTo("aa");
        assertThat(t.f0).isEqualTo("aa");

        FieldAccessor<Tuple2<String, Tuple3<Integer, Long, Double>>, Double> f1f2 =
                FieldAccessorFactory.getAccessor(tpeInfo, "f1.f2", null);
        assertThat(f1f2.getFieldType().getTypeClass()).isEqualTo(Double.class);
        assertThat(f1f2.get(t)).isEqualTo(2.0);
        assertThat(t.f1.f2).isEqualTo(2.0);
        t = f1f2.set(t, 3.0);
        assertThat(f1f2.get(t)).isEqualTo(3.0);
        assertThat(t.f1.f2).isEqualTo(3.0);
        assertThat(f0.get(t)).isEqualTo("aa");
        assertThat(t.f0).isEqualTo("aa");

        FieldAccessor<Tuple2<String, Tuple3<Integer, Long, Double>>, Tuple3<Integer, Long, Double>>
                f1 = FieldAccessorFactory.getAccessor(tpeInfo, "f1", null);
        assertThat(f1.getFieldType().getTypeClass()).isEqualTo(Tuple3.class);
        assertThat(f1.get(t)).isEqualTo(Tuple3.of(5, 9L, 3.0));
        assertThat(t.f1).isEqualTo(Tuple3.of(5, 9L, 3.0));
        t = f1.set(t, Tuple3.of(8, 12L, 4.0));
        assertThat(f1.get(t)).isEqualTo(Tuple3.of(8, 12L, 4.0));
        assertThat(t.f1).isEqualTo(Tuple3.of(8, 12L, 4.0));
        assertThat(f0.get(t)).isEqualTo("aa");
        assertThat(t.f0).isEqualTo("aa");

        FieldAccessor<Tuple2<String, Tuple3<Integer, Long, Double>>, Tuple3<Integer, Long, Double>>
                f1n = FieldAccessorFactory.getAccessor(tpeInfo, 1, null);
        assertThat(f1n.getFieldType().getTypeClass()).isEqualTo(Tuple3.class);
        assertThat(f1n.get(t)).isEqualTo(Tuple3.of(8, 12L, 4.0));
        assertThat(t.f1).isEqualTo(Tuple3.of(8, 12L, 4.0));
        t = f1n.set(t, Tuple3.of(10, 13L, 5.0));
        assertThat(f1n.get(t)).isEqualTo(Tuple3.of(10, 13L, 5.0));
        assertThat(f1.get(t)).isEqualTo(Tuple3.of(10, 13L, 5.0));
        assertThat(t.f1).isEqualTo(Tuple3.of(10, 13L, 5.0));
        assertThat(f0.get(t)).isEqualTo("aa");
        assertThat(t.f0).isEqualTo("aa");
    }

    @Test
    void testIllegalTupleField() {
        assertThatThrownBy(
                        () ->
                                FieldAccessorFactory.getAccessor(
                                        TupleTypeInfo.getBasicTupleTypeInfo(
                                                Integer.class, Integer.class),
                                        2,
                                        null))
                .isInstanceOf(CompositeType.InvalidFieldReferenceException.class);
    }

    /** POJO. */
    public static class Foo {
        public int x;
        public Tuple2<String, Long> t;
        public Short y;

        public Foo() {}

        public Foo(int x, Tuple2<String, Long> t, Short y) {
            this.x = x;
            this.t = t;
            this.y = y;
        }
    }

    @Test
    void testTupleInPojoInTuple() {
        Tuple2<String, Foo> t = Tuple2.of("aa", new Foo(8, Tuple2.of("ddd", 9L), (short) 2));
        TupleTypeInfo<Tuple2<String, Foo>> tpeInfo =
                (TupleTypeInfo<Tuple2<String, Foo>>) TypeExtractor.getForObject(t);

        FieldAccessor<Tuple2<String, Foo>, Long> f1tf1 =
                FieldAccessorFactory.getAccessor(tpeInfo, "f1.t.f1", null);
        assertThat(f1tf1.getFieldType().getTypeClass()).isEqualTo(Long.class);
        assertThat(f1tf1.get(t)).isEqualTo(9L);
        assertThat(t.f1.t.f1).isEqualTo(9L);
        t = f1tf1.set(t, 12L);
        assertThat(f1tf1.get(t)).isEqualTo(12L);
        assertThat(t.f1.t.f1).isEqualTo(12L);

        FieldAccessor<Tuple2<String, Foo>, String> f1tf0 =
                FieldAccessorFactory.getAccessor(tpeInfo, "f1.t.f0", null);
        assertThat(f1tf0.getFieldType().getTypeClass()).isEqualTo(String.class);
        assertThat(f1tf0.get(t)).isEqualTo("ddd");
        assertThat(t.f1.t.f0).isEqualTo("ddd");
        t = f1tf0.set(t, "alma");
        assertThat(f1tf0.get(t)).isEqualTo("alma");
        assertThat(t.f1.t.f0).isEqualTo("alma");

        FieldAccessor<Tuple2<String, Foo>, Foo> f1 =
                FieldAccessorFactory.getAccessor(tpeInfo, "f1", null);
        FieldAccessor<Tuple2<String, Foo>, Foo> f1n =
                FieldAccessorFactory.getAccessor(tpeInfo, 1, null);
        assertThat(f1.getFieldType().getTypeClass()).isEqualTo(Foo.class);
        assertThat(f1n.getFieldType().getTypeClass()).isEqualTo(Foo.class);
        assertThat(f1.get(t).t).isEqualTo(Tuple2.of("alma", 12L));
        assertThat(f1n.get(t).t).isEqualTo(Tuple2.of("alma", 12L));
        assertThat(t.f1.t).isEqualTo(Tuple2.of("alma", 12L));
        Foo newFoo = new Foo(8, Tuple2.of("ddd", 9L), (short) 2);
        f1.set(t, newFoo);
        assertThat(f1.get(t)).isEqualTo(newFoo);
        assertThat(f1n.get(t)).isEqualTo(newFoo);
        assertThat(t.f1).isEqualTo(newFoo);
    }

    @Test
    void testIllegalTupleInPojoInTuple() {
        Tuple2<String, Foo> t = Tuple2.of("aa", new Foo(8, Tuple2.of("ddd", 9L), (short) 2));
        TupleTypeInfo<Tuple2<String, Foo>> tpeInfo =
                (TupleTypeInfo<Tuple2<String, Foo>>) TypeExtractor.getForObject(t);

        assertThatThrownBy(
                        () ->
                                FieldAccessorFactory.getAccessor(
                                        tpeInfo, "illegal.illegal.illegal", null))
                .isInstanceOf(CompositeType.InvalidFieldReferenceException.class);
    }

    /** POJO for testing field access. */
    public static class Inner {
        public long x;
        public boolean b;

        public Inner() {}

        public Inner(long x) {
            this.x = x;
        }

        public Inner(long x, boolean b) {
            this.x = x;
            this.b = b;
        }

        @Override
        public String toString() {
            return ((Long) x).toString() + ", " + b;
        }
    }

    /** POJO containing POJO. */
    public static class Outer {
        public int a;
        public Inner i;
        public short b;

        public Outer() {}

        public Outer(int a, Inner i, short b) {
            this.a = a;
            this.i = i;
            this.b = b;
        }

        @Override
        public String toString() {
            return a + ", " + i.toString() + ", " + b;
        }
    }

    @Test
    void testPojoInPojo() {
        Outer o = new Outer(10, new Inner(4L), (short) 12);
        PojoTypeInfo<Outer> tpeInfo = (PojoTypeInfo<Outer>) TypeInformation.of(Outer.class);

        FieldAccessor<Outer, Long> fix = FieldAccessorFactory.getAccessor(tpeInfo, "i.x", null);
        assertThat(fix.getFieldType().getTypeClass()).isEqualTo(Long.class);
        assertThat(fix.get(o)).isEqualTo(4L);
        assertThat(o.i.x).isEqualTo(4L);
        o = fix.set(o, 22L);
        assertThat(fix.get(o)).isEqualTo(22L);
        assertThat(o.i.x).isEqualTo(22L);

        FieldAccessor<Outer, Inner> fi = FieldAccessorFactory.getAccessor(tpeInfo, "i", null);
        assertThat(fi.getFieldType().getTypeClass()).isEqualTo(Inner.class);
        assertThat(fi.get(o).x).isEqualTo(22L);
        assertThat(fix.get(o)).isEqualTo(22L);
        assertThat(o.i.x).isEqualTo(22L);
        o = fi.set(o, new Inner(30L));
        assertThat(fi.get(o).x).isEqualTo(30L);
        assertThat(fix.get(o)).isEqualTo(30L);
        assertThat(o.i.x).isEqualTo(30L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testArray() {
        int[] a = new int[] {3, 5};
        FieldAccessor<int[], Integer> fieldAccessor =
                (FieldAccessor<int[], Integer>)
                        (Object)
                                FieldAccessorFactory.getAccessor(
                                        PrimitiveArrayTypeInfo.getInfoFor(a.getClass()), 1, null);

        assertThat(fieldAccessor.getFieldType().getTypeClass()).isEqualTo(Integer.class);

        assertThat(fieldAccessor.get(a)).isEqualTo(a[1]);

        a = fieldAccessor.set(a, 6);
        assertThat(fieldAccessor.get(a)).isEqualTo(a[1]);

        Integer[] b = new Integer[] {3, 5};
        FieldAccessor<Integer[], Integer> fieldAccessor2 =
                (FieldAccessor<Integer[], Integer>)
                        (Object)
                                FieldAccessorFactory.getAccessor(
                                        BasicArrayTypeInfo.getInfoFor(b.getClass()), 1, null);

        assertThat(fieldAccessor2.getFieldType().getTypeClass()).isEqualTo(Integer.class);

        assertThat(fieldAccessor2.get(b)).isEqualTo(b[1]);

        b = fieldAccessor2.set(b, 6);
        assertThat(fieldAccessor2.get(b)).isEqualTo(b[1]);
    }

    /** POJO with array. */
    public static class ArrayInPojo {
        public long x;
        public int[] arr;
        public int y;

        public ArrayInPojo() {}

        public ArrayInPojo(long x, int[] arr, int y) {
            this.x = x;
            this.arr = arr;
            this.y = y;
        }
    }

    @Test
    void testArrayInPojo() {
        ArrayInPojo o = new ArrayInPojo(10L, new int[] {3, 4, 5}, 12);
        PojoTypeInfo<ArrayInPojo> tpeInfo =
                (PojoTypeInfo<ArrayInPojo>) TypeInformation.of(ArrayInPojo.class);

        FieldAccessor<ArrayInPojo, Integer> fix =
                FieldAccessorFactory.getAccessor(tpeInfo, "arr.1", null);
        assertThat(fix.getFieldType().getTypeClass()).isEqualTo(Integer.class);
        assertThat(fix.get(o)).isEqualTo(4);
        assertThat(o.arr[1]).isEqualTo(4L);
        o = fix.set(o, 8);
        assertThat(fix.get(o)).isEqualTo(8);
        assertThat(o.arr[1]).isEqualTo(8);
    }

    @Test
    void testBasicType() {
        Long x = 7L;
        TypeInformation<Long> tpeInfo = BasicTypeInfo.LONG_TYPE_INFO;

        FieldAccessor<Long, Long> f = FieldAccessorFactory.getAccessor(tpeInfo, 0, null);
        assertThat(f.getFieldType().getTypeClass()).isEqualTo(Long.class);
        assertThat(f.get(x)).isEqualTo(7L);
        x = f.set(x, 12L);
        assertThat(f.get(x)).isEqualTo(12L);
        assertThat(x).isEqualTo(12L);

        FieldAccessor<Long, Long> f2 = FieldAccessorFactory.getAccessor(tpeInfo, "*", null);
        assertThat(f2.getFieldType().getTypeClass()).isEqualTo(Long.class);
        assertThat(f2.get(x)).isEqualTo(12L);
        x = f2.set(x, 14L);
        assertThat(f2.get(x)).isEqualTo(14L);
        assertThat(x).isEqualTo(14L);
    }

    @Test
    void testIllegalBasicType1() {
        TypeInformation<Long> tpeInfo = BasicTypeInfo.LONG_TYPE_INFO;

        assertThatThrownBy(() -> FieldAccessorFactory.getAccessor(tpeInfo, 1, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testIllegalBasicType2() {
        TypeInformation<Long> tpeInfo = BasicTypeInfo.LONG_TYPE_INFO;

        assertThatThrownBy(() -> FieldAccessorFactory.getAccessor(tpeInfo, "foo", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Validates that no ClassCastException happens should not fail e.g. like in FLINK-8255. */
    @Test
    void testRowTypeInfo() {
        TypeInformation<?>[] typeList =
                new TypeInformation<?>[] {
                    new RowTypeInfo(BasicTypeInfo.SHORT_TYPE_INFO, BasicTypeInfo.BIG_DEC_TYPE_INFO)
                };

        String[] fieldNames = new String[] {"row"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(typeList, fieldNames);

        assertThatThrownBy(() -> FieldAccessorFactory.getAccessor(rowTypeInfo, "row.0", null))
                .isInstanceOf(CompositeType.InvalidFieldReferenceException.class);
    }
}
