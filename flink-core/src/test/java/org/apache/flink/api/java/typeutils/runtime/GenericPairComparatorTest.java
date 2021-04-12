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

import org.apache.flink.api.common.typeutils.GenericPairComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleComparator;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.runtime.tuple.base.TuplePairComparatorTestBase;

public class GenericPairComparatorTest
        extends TuplePairComparatorTestBase<
                Tuple3<Integer, String, Double>, Tuple4<Integer, Float, Long, Double>> {

    @SuppressWarnings("unchecked")
    private Tuple3<Integer, String, Double>[] dataISD =
            new Tuple3[] {
                new Tuple3<Integer, String, Double>(4, "hello", 20.0),
                new Tuple3<Integer, String, Double>(4, "world", 23.2),
                new Tuple3<Integer, String, Double>(5, "hello", 18.0),
                new Tuple3<Integer, String, Double>(5, "world", 19.2),
                new Tuple3<Integer, String, Double>(6, "hello", 16.0),
                new Tuple3<Integer, String, Double>(6, "world", 17.2),
                new Tuple3<Integer, String, Double>(7, "hello", 14.0),
                new Tuple3<Integer, String, Double>(7, "world", 15.2)
            };

    @SuppressWarnings("unchecked")
    private Tuple4<Integer, Float, Long, Double>[] dataIDL =
            new Tuple4[] {
                new Tuple4<Integer, Float, Long, Double>(4, 0.11f, 14L, 20.0),
                new Tuple4<Integer, Float, Long, Double>(4, 0.221f, 15L, 23.2),
                new Tuple4<Integer, Float, Long, Double>(5, 0.33f, 15L, 18.0),
                new Tuple4<Integer, Float, Long, Double>(5, 0.44f, 20L, 19.2),
                new Tuple4<Integer, Float, Long, Double>(6, 0.55f, 20L, 16.0),
                new Tuple4<Integer, Float, Long, Double>(6, 0.66f, 29L, 17.2),
                new Tuple4<Integer, Float, Long, Double>(7, 0.77f, 29L, 14.0),
                new Tuple4<Integer, Float, Long, Double>(7, 0.88f, 34L, 15.2)
            };

    @SuppressWarnings("rawtypes")
    @Override
    protected GenericPairComparator<
                    Tuple3<Integer, String, Double>, Tuple4<Integer, Float, Long, Double>>
            createComparator(boolean ascending) {
        int[] fields1 = new int[] {0, 2};
        int[] fields2 = new int[] {0, 3};
        TypeComparator[] comps1 =
                new TypeComparator[] {
                    new IntComparator(ascending), new DoubleComparator(ascending)
                };
        TypeComparator[] comps2 =
                new TypeComparator[] {
                    new IntComparator(ascending), new DoubleComparator(ascending)
                };
        TypeSerializer[] sers1 =
                new TypeSerializer[] {IntSerializer.INSTANCE, DoubleSerializer.INSTANCE};
        TypeSerializer[] sers2 =
                new TypeSerializer[] {IntSerializer.INSTANCE, DoubleSerializer.INSTANCE};
        TypeComparator<Tuple3<Integer, String, Double>> comp1 =
                new TupleComparator<Tuple3<Integer, String, Double>>(fields1, comps1, sers1);
        TypeComparator<Tuple4<Integer, Float, Long, Double>> comp2 =
                new TupleComparator<Tuple4<Integer, Float, Long, Double>>(fields2, comps2, sers2);
        return new GenericPairComparator<
                Tuple3<Integer, String, Double>, Tuple4<Integer, Float, Long, Double>>(
                comp1, comp2);
    }

    @Override
    protected Tuple2<Tuple3<Integer, String, Double>[], Tuple4<Integer, Float, Long, Double>[]>
            getSortedTestData() {
        return new Tuple2<
                Tuple3<Integer, String, Double>[], Tuple4<Integer, Float, Long, Double>[]>(
                dataISD, dataIDL);
    }
}
