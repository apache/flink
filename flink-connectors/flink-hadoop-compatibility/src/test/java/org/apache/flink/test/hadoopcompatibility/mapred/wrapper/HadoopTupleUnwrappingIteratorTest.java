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

package org.apache.flink.test.hadoopcompatibility.mapred.wrapper;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.WritableSerializer;
import org.apache.flink.hadoopcompatibility.mapred.wrapper.HadoopTupleUnwrappingIterator;

import org.apache.hadoop.io.IntWritable;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link HadoopTupleUnwrappingIterator}. */
class HadoopTupleUnwrappingIteratorTest {

    @Test
    void testValueIterator() {

        HadoopTupleUnwrappingIterator<IntWritable, IntWritable> valIt =
                new HadoopTupleUnwrappingIterator<IntWritable, IntWritable>(
                        new WritableSerializer<IntWritable>(IntWritable.class));

        // many values

        ArrayList<Tuple2<IntWritable, IntWritable>> tList =
                new ArrayList<Tuple2<IntWritable, IntWritable>>();
        tList.add(new Tuple2<IntWritable, IntWritable>(new IntWritable(1), new IntWritable(1)));
        tList.add(new Tuple2<IntWritable, IntWritable>(new IntWritable(1), new IntWritable(2)));
        tList.add(new Tuple2<IntWritable, IntWritable>(new IntWritable(1), new IntWritable(3)));
        tList.add(new Tuple2<IntWritable, IntWritable>(new IntWritable(1), new IntWritable(4)));
        tList.add(new Tuple2<IntWritable, IntWritable>(new IntWritable(1), new IntWritable(5)));
        tList.add(new Tuple2<IntWritable, IntWritable>(new IntWritable(1), new IntWritable(6)));
        tList.add(new Tuple2<IntWritable, IntWritable>(new IntWritable(1), new IntWritable(7)));
        tList.add(new Tuple2<IntWritable, IntWritable>(new IntWritable(1), new IntWritable(8)));

        int expectedKey = 1;
        int[] expectedValues = new int[] {1, 2, 3, 4, 5, 6, 7, 8};

        valIt.set(tList.iterator());
        assertThat(valIt.getCurrentKey().get()).isEqualTo(expectedKey);
        for (int expectedValue : expectedValues) {
            assertThat(valIt.hasNext()).isTrue();
            assertThat(valIt.next().get()).isEqualTo(expectedValue);
            assertThat(valIt.getCurrentKey().get()).isEqualTo(expectedKey);
        }
        assertThat(valIt.hasNext()).isFalse();
        assertThat(valIt.getCurrentKey().get()).isEqualTo(expectedKey);

        // one value

        tList.clear();
        tList.add(new Tuple2<IntWritable, IntWritable>(new IntWritable(2), new IntWritable(10)));

        expectedKey = 2;
        expectedValues = new int[] {10};

        valIt.set(tList.iterator());
        assertThat(valIt.getCurrentKey().get()).isEqualTo(expectedKey);
        for (int expectedValue : expectedValues) {
            assertThat(valIt.hasNext()).isTrue();
            assertThat(valIt.next().get()).isEqualTo(expectedValue);
            assertThat(valIt.getCurrentKey().get()).isEqualTo(expectedKey);
        }
        assertThat(valIt.hasNext()).isFalse();
        assertThat(valIt.getCurrentKey().get()).isEqualTo(expectedKey);

        // more values

        tList.clear();
        tList.add(new Tuple2<IntWritable, IntWritable>(new IntWritable(3), new IntWritable(10)));
        tList.add(new Tuple2<IntWritable, IntWritable>(new IntWritable(3), new IntWritable(4)));
        tList.add(new Tuple2<IntWritable, IntWritable>(new IntWritable(3), new IntWritable(7)));
        tList.add(new Tuple2<IntWritable, IntWritable>(new IntWritable(3), new IntWritable(9)));
        tList.add(new Tuple2<IntWritable, IntWritable>(new IntWritable(4), new IntWritable(21)));

        expectedKey = 3;
        expectedValues = new int[] {10, 4, 7, 9, 21};

        valIt.set(tList.iterator());
        assertThat(valIt.hasNext()).isTrue();
        assertThat(valIt.getCurrentKey().get()).isEqualTo(expectedKey);
        for (int expectedValue : expectedValues) {
            assertThat(valIt.hasNext()).isTrue();
            assertThat(valIt.next().get()).isEqualTo(expectedValue);
            assertThat(valIt.getCurrentKey().get()).isEqualTo(expectedKey);
        }
        assertThat(valIt.hasNext()).isFalse();
        assertThat(valIt.getCurrentKey().get()).isEqualTo(expectedKey);

        // no has next calls

        tList.clear();
        tList.add(new Tuple2<IntWritable, IntWritable>(new IntWritable(4), new IntWritable(5)));
        tList.add(new Tuple2<IntWritable, IntWritable>(new IntWritable(4), new IntWritable(8)));
        tList.add(new Tuple2<IntWritable, IntWritable>(new IntWritable(4), new IntWritable(42)));
        tList.add(new Tuple2<IntWritable, IntWritable>(new IntWritable(4), new IntWritable(-1)));
        tList.add(new Tuple2<IntWritable, IntWritable>(new IntWritable(4), new IntWritable(0)));

        expectedKey = 4;
        expectedValues = new int[] {5, 8, 42, -1, 0};

        valIt.set(tList.iterator());
        assertThat(valIt.getCurrentKey().get()).isEqualTo(expectedKey);
        for (int expectedValue : expectedValues) {
            assertThat(valIt.next().get()).isEqualTo(expectedValue);
        }
        assertThatThrownBy(() -> valIt.next()).isInstanceOf(NoSuchElementException.class);
        assertThat(valIt.hasNext()).isFalse();
        assertThat(valIt.getCurrentKey().get()).isEqualTo(expectedKey);
    }
}
