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
package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.BeforeClass;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;

public class RowComparatorTest extends ComparatorTestBase<Row> {

    private static final RowTypeInfo typeInfo =
            new RowTypeInfo(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    new TupleTypeInfo<Tuple3<Integer, Boolean, Short>>(
                            BasicTypeInfo.INT_TYPE_INFO,
                            BasicTypeInfo.BOOLEAN_TYPE_INFO,
                            BasicTypeInfo.SHORT_TYPE_INFO),
                    TypeExtractor.createTypeInfo(MyPojo.class));

    private static MyPojo testPojo1 = new MyPojo();
    private static MyPojo testPojo2 = new MyPojo();
    private static MyPojo testPojo3 = new MyPojo();

    private static final Row[] data =
            new Row[] {
                createRow(RowKind.INSERT, null, null, null, null, null),
                createRow(RowKind.INSERT, 0, null, null, null, null),
                createRow(RowKind.INSERT, 0, 0.0, null, null, null),
                createRow(RowKind.INSERT, 0, 0.0, "a", null, null),
                createRow(RowKind.INSERT, 1, 0.0, "a", null, null),
                createRow(RowKind.INSERT, 1, 1.0, "a", null, null),
                createRow(RowKind.INSERT, 1, 1.0, "b", null, null),
                createRow(
                        RowKind.UPDATE_AFTER, 1, 1.0, "b", new Tuple3<>(1, false, (short) 2), null),
                createRow(
                        RowKind.UPDATE_AFTER, 1, 1.0, "b", new Tuple3<>(2, false, (short) 2), null),
                createRow(
                        RowKind.UPDATE_AFTER, 1, 1.0, "b", new Tuple3<>(2, true, (short) 2), null),
                createRow(
                        RowKind.UPDATE_AFTER, 1, 1.0, "b", new Tuple3<>(2, true, (short) 3), null),
                createRow(RowKind.DELETE, 1, 1.0, "b", new Tuple3<>(2, true, (short) 3), testPojo1),
                createRow(RowKind.DELETE, 1, 1.0, "b", new Tuple3<>(2, true, (short) 3), testPojo2),
                createRow(RowKind.DELETE, 1, 1.0, "b", new Tuple3<>(2, true, (short) 3), testPojo3)
            };

    @BeforeClass
    public static void init() {
        // TODO we cannot test null here as PojoComparator has no support for null keys
        testPojo1.name = "";
        testPojo2.name = "Test1";
        testPojo3.name = "Test2";
    }

    @Override
    protected void deepEquals(String message, Row should, Row is) {
        int arity = should.getArity();
        assertEquals(message, arity, is.getArity());
        for (int i = 0; i < arity; i++) {
            Object copiedValue = should.getField(i);
            Object element = is.getField(i);
            assertEquals(message, element, copiedValue);
        }
    }

    @Override
    protected TypeComparator<Row> createComparator(boolean ascending) {
        return typeInfo.createComparator(
                new int[] {0, 1, 2, 3, 4, 5, 6},
                new boolean[] {
                    ascending, ascending, ascending, ascending, ascending, ascending, ascending
                },
                0,
                new ExecutionConfig());
    }

    @Override
    protected TypeSerializer<Row> createSerializer() {
        return typeInfo.createSerializer(new ExecutionConfig());
    }

    @Override
    protected Row[] getSortedTestData() {
        return data;
    }

    @Override
    protected boolean supportsNullKeys() {
        return true;
    }

    private static Row createRow(
            RowKind kind, Object f0, Object f1, Object f2, Object f3, Object f4) {
        Row row = new Row(kind, 5);
        row.setField(0, f0);
        row.setField(1, f1);
        row.setField(2, f2);
        row.setField(3, f3);
        row.setField(4, f4);
        return row;
    }

    public static class MyPojo implements Serializable, Comparable<MyPojo> {
        // we cannot use null because the PojoComparator does not support null properly
        public String name = "";

        @Override
        public int compareTo(MyPojo o) {
            if (name == null && o.name == null) {
                return 0;
            } else if (name == null) {
                return -1;
            } else if (o.name == null) {
                return 1;
            } else {
                return name.compareTo(o.name);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MyPojo myPojo = (MyPojo) o;

            return name != null ? name.equals(myPojo.name) : myPojo.name == null;
        }
    }
}
