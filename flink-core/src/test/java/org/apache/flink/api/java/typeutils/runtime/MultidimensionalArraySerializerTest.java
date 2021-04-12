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
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.SerializerTestInstance;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.junit.Test;

/** A serialization test for multidimensional arrays. */
public class MultidimensionalArraySerializerTest {

    @Test
    public void testStringArray() {
        String[][] array = new String[][] {{null, "b"}, {"c", "d"}, {"e", "f"}, {"g", "h"}, null};
        TypeInformation<String[][]> ti = TypeExtractor.getForClass(String[][].class);

        SerializerTestInstance<String[][]> testInstance =
                new SerializerTestInstance<String[][]>(
                        ti.createSerializer(new ExecutionConfig()), String[][].class, -1, array);
        testInstance.testAll();
    }

    @Test
    public void testPrimitiveArray() {
        int[][] array = new int[][] {{12, 1}, {48, 42}, {23, 80}, {484, 849}, {987, 4}};
        TypeInformation<int[][]> ti = TypeExtractor.getForClass(int[][].class);

        SerializerTestInstance<int[][]> testInstance =
                new SerializerTestInstance<int[][]>(
                        ti.createSerializer(new ExecutionConfig()), int[][].class, -1, array);
        testInstance.testAll();
    }

    public static class MyPojo {
        public String field1;
        public int field2;

        public MyPojo(String field1, int field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof MyPojo)) {
                return false;
            }
            MyPojo other = (MyPojo) obj;
            return ((field1 == null && other.field1 == null)
                            || (field1 != null && field1.equals(other.field1)))
                    && field2 == other.field2;
        }
    }

    @Test
    public void testObjectArrays() {
        Integer[][] array = new Integer[][] {{0, 1}, null, {null, 42}};
        TypeInformation<Integer[][]> ti = TypeExtractor.getForClass(Integer[][].class);

        SerializerTestInstance<Integer[][]> testInstance =
                new SerializerTestInstance<Integer[][]>(
                        ti.createSerializer(new ExecutionConfig()), Integer[][].class, -1, array);
        testInstance.testAll();

        MyPojo[][] array2 =
                new MyPojo[][] {
                    {new MyPojo(null, 42), new MyPojo("test2", -1)}, {null, null}, null
                };
        TypeInformation<MyPojo[][]> ti2 = TypeExtractor.getForClass(MyPojo[][].class);

        SerializerTestInstance<MyPojo[][]> testInstance2 =
                new SerializerTestInstance<MyPojo[][]>(
                        ti2.createSerializer(new ExecutionConfig()), MyPojo[][].class, -1, array2);
        testInstance2.testAll();
    }

    public static class MyGenericPojo<T> {
        public T[][] field;

        public MyGenericPojo() {
            // nothing to do
        }

        public MyGenericPojo(T[][] field) {
            this.field = field;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof MyGenericPojo)) {
                return false;
            }
            MyGenericPojo<?> other = (MyGenericPojo<?>) obj;
            return (field == null && other.field == null)
                    || (field != null && field.length == other.field.length);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testGenericObjectArrays() {
        MyGenericPojo<String>[][] array =
                (MyGenericPojo<String>[][])
                        new MyGenericPojo[][] {
                            {
                                new MyGenericPojo<String>(new String[][] {{"a", "b"}, {"c", "d"}}),
                                null
                            }
                        };

        TypeInformation<MyGenericPojo<String>[][]> ti =
                TypeInformation.of(new TypeHint<MyGenericPojo<String>[][]>() {});

        SerializerTestInstance testInstance =
                new SerializerTestInstance(
                        ti.createSerializer(new ExecutionConfig()),
                        MyGenericPojo[][].class,
                        -1,
                        (Object) array);
        testInstance.testAll();
    }
}
