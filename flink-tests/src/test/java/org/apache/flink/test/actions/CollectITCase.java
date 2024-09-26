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

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MultipleProgramsTestBaseJUnit4;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Tests the methods that bring elements back to the client driver program. */
@RunWith(Parameterized.class)
public class CollectITCase extends MultipleProgramsTestBaseJUnit4 {

    public CollectITCase(TestExecutionMode mode) {
        super(mode);
    }

    @Test
    public void testSimple() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Integer[] input = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        DataStreamSource<Integer> data = env.fromData(input);
        List<Integer> collectedResult = CollectionUtil.iteratorToList(data.executeAndCollect());

        // count
        long numEntries = collectedResult.size();
        assertEquals(10, numEntries);

        // collect
        assertArrayEquals(input, collectedResult.toArray());
    }

    @Test
    public void testAdvanced() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableObjectReuse();

        DataStreamSource<Long> data = env.fromSequence(1, 10);
        SingleOutputStreamOperator<Long> data2 = data.map(x -> x + 10).filter(x -> x % 2 == 0);
        CloseableIterator<Long> longCloseableIterator = data2.executeAndCollect();
        List<Long> collectedResult = CollectionUtil.iteratorToList(longCloseableIterator);
        Long[] expectedOutput = {12L, 14L, 16L, 18L, 20L};

        // count
        long numEntries = collectedResult.size();
        assertEquals(expectedOutput.length, numEntries);

        // collect
        assertArrayEquals(expectedOutput, collectedResult.stream().sorted().toArray());
    }
}
