/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.windowing.delta.extractor;

import org.apache.flink.api.java.tuple.*;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link FieldsFromTuple}. */
public class FieldsFromTupleTest {

    private double[] testDouble;

    @Before
    public void init() {
        testDouble = new double[Tuple.MAX_ARITY];
        for (int i = 0; i < Tuple.MAX_ARITY; i++) {
            testDouble[i] = i;
        }
    }

    @Test
    public void testUserSpecifiedOrder() throws InstantiationException, IllegalAccessException {
        Tuple currentTuple = (Tuple) CLASSES[Tuple.MAX_ARITY - 1].newInstance();
        for (int i = 0; i < Tuple.MAX_ARITY; i++) {
            currentTuple.setField(testDouble[i], i);
        }

        double[] expected = {
            testDouble[5], testDouble[3], testDouble[6], testDouble[7], testDouble[0]
        };
        arrayEqualityCheck(expected, new FieldsFromTuple(5, 3, 6, 7, 0).extract(currentTuple));

        double[] expected2 = {testDouble[0], testDouble[Tuple.MAX_ARITY - 1]};
        arrayEqualityCheck(
                expected2, new FieldsFromTuple(0, Tuple.MAX_ARITY - 1).extract(currentTuple));

        double[] expected3 = {testDouble[Tuple.MAX_ARITY - 1], testDouble[0]};
        arrayEqualityCheck(
                expected3, new FieldsFromTuple(Tuple.MAX_ARITY - 1, 0).extract(currentTuple));

        double[] expected4 = {
            testDouble[13],
            testDouble[4],
            testDouble[5],
            testDouble[4],
            testDouble[2],
            testDouble[8],
            testDouble[6],
            testDouble[2],
            testDouble[8],
            testDouble[3],
            testDouble[5],
            testDouble[2],
            testDouble[16],
            testDouble[4],
            testDouble[3],
            testDouble[2],
            testDouble[6],
            testDouble[4],
            testDouble[7],
            testDouble[4],
            testDouble[2],
            testDouble[8],
            testDouble[7],
            testDouble[2]
        };
        arrayEqualityCheck(
                expected4,
                new FieldsFromTuple(
                                13, 4, 5, 4, 2, 8, 6, 2, 8, 3, 5, 2, 16, 4, 3, 2, 6, 4, 7, 4, 2, 8,
                                7, 2)
                        .extract(currentTuple));
    }

    private void arrayEqualityCheck(double[] array1, double[] array2) {
        assertEquals(array1.length, array2.length, "The result arrays must have the same length");
        for (int i = 0; i < array1.length; i++) {
            assertEquals("Unequal fields at position " + i, array1[i], array2[i], 0d);
        }
    }

    private static final Class<?>[] CLASSES =
            new Class<?>[] {
                Tuple1.class,
                Tuple2.class,
                Tuple3.class,
                Tuple4.class,
                Tuple5.class,
                Tuple6.class,
                Tuple7.class,
                Tuple8.class,
                Tuple9.class,
                Tuple10.class,
                Tuple11.class,
                Tuple12.class,
                Tuple13.class,
                Tuple14.class,
                Tuple15.class,
                Tuple16.class,
                Tuple17.class,
                Tuple18.class,
                Tuple19.class,
                Tuple20.class,
                Tuple21.class,
                Tuple22.class,
                Tuple23.class,
                Tuple24.class,
                Tuple25.class
            };
}
