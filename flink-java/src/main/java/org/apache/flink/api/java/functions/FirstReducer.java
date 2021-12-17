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

package org.apache.flink.api.java.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

/**
 * Reducer that only emits the first N elements in a group.
 *
 * @param <T>
 */
@Internal
public class FirstReducer<T> implements GroupReduceFunction<T, T>, GroupCombineFunction<T, T> {
    private static final long serialVersionUID = 1L;

    private final int count;

    public FirstReducer(int n) {
        this.count = n;
    }

    @Override
    public void reduce(Iterable<T> values, Collector<T> out) throws Exception {

        int emitCnt = 0;
        for (T val : values) {
            out.collect(val);

            emitCnt++;
            if (emitCnt == count) {
                break;
            }
        }
    }

    @Override
    public void combine(Iterable<T> values, Collector<T> out) throws Exception {
        reduce(values, out);
    }
}
