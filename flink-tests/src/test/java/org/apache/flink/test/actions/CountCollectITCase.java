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

package org.apache.flink.test.actions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.test.util.MultipleProgramsTestBase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Tests the methods that bring elements back to the client driver program. */
@RunWith(Parameterized.class)
public class CountCollectITCase extends MultipleProgramsTestBase {

    public CountCollectITCase(TestExecutionMode mode) {
        super(mode);
    }

    @Test
    public void testSimple() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Integer[] input = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        DataSet<Integer> data = env.fromElements(input);

        // count
        long numEntries = data.count();
        assertEquals(10, numEntries);

        // collect
        ArrayList<Integer> list = (ArrayList<Integer>) data.collect();
        assertArrayEquals(input, list.toArray());
    }

    @Test
    public void testAdvanced() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableObjectReuse();

        DataSet<Integer> data = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        DataSet<Integer> data2 = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        DataSet<Tuple2<Integer, Integer>> data3 = data.cross(data2);

        // count
        long numEntries = data3.count();
        assertEquals(100, numEntries);

        // collect
        ArrayList<Tuple2<Integer, Integer>> list =
                (ArrayList<Tuple2<Integer, Integer>>) data3.collect();

        // set expected entries in a hash map to true
        HashMap<Tuple2<Integer, Integer>, Boolean> expected =
                new HashMap<Tuple2<Integer, Integer>, Boolean>();
        for (int i = 1; i <= 10; i++) {
            for (int j = 1; j <= 10; j++) {
                expected.put(new Tuple2<Integer, Integer>(i, j), true);
            }
        }

        // check if all entries are contained in the hash map
        for (int i = 0; i < 100; i++) {
            Tuple2<Integer, Integer> element = list.get(i);
            assertEquals(expected.get(element), true);
            expected.remove(element);
        }
    }
}
