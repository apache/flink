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

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple13;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.api.java.tuple.Tuple17;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.tuple.Tuple19;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple20;
import org.apache.flink.api.java.tuple.Tuple21;
import org.apache.flink.api.java.tuple.Tuple22;
import org.apache.flink.api.java.tuple.Tuple23;
import org.apache.flink.api.java.tuple.Tuple24;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link FieldFromTuple}. */
public class FieldFromTupleTest {

    private String[] testStrings;

    @Before
    public void init() {
        testStrings = new String[Tuple.MAX_ARITY];
        for (int i = 0; i < Tuple.MAX_ARITY; i++) {
            testStrings[i] = Integer.toString(i);
        }
    }

    @Test
    public void testSingleFieldExtraction() throws InstantiationException, IllegalAccessException {
        // extract single fields
        for (int i = 0; i < Tuple.MAX_ARITY; i++) {
            Tuple current = (Tuple) CLASSES[i].newInstance();
            for (int j = 0; j < i; j++) {
                current.setField(testStrings[j], j);
            }
            for (int j = 0; j < i; j++) {
                assertEquals(testStrings[j], new FieldFromTuple<String>(j).extract(current));
            }
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
