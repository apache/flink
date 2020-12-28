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

package org.apache.flink.streaming.runtime.operators.sink;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/** Test {@link StreamingCommitterState}. */
public class StreamingCommitterStateTest {

    @Test
    public void constructFromMap() {
        final NavigableMap<Long, List<Integer>> r = new TreeMap<>();
        final List<Integer> expectedList =
                Arrays.asList(0, 1, 2, 3, 10, 11, 12, 13, 30, 31, 32, 33);

        r.put(1L, Arrays.asList(10, 11, 12, 13));
        r.put(0L, Arrays.asList(0, 1, 2, 3));
        r.put(3L, Arrays.asList(30, 31, 32, 33));

        final StreamingCommitterState<Integer> streamingCommitterState =
                new StreamingCommitterState<>(r);

        assertThat(streamingCommitterState.getCommittables(), equalTo(expectedList));
    }
}
