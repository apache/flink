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

package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple2;

public class FullOuterJoinTaskTest extends AbstractOuterJoinTaskTest {

    public FullOuterJoinTaskTest(ExecutionConfig config) {
        super(config);
    }

    @Override
    protected DriverStrategy getSortDriverStrategy() {
        return DriverStrategy.FULL_OUTER_MERGE;
    }

    @Override
    protected int calculateExpectedCount(int keyCnt1, int valCnt1, int keyCnt2, int valCnt2) {
        return valCnt1 * valCnt2 * Math.min(keyCnt1, keyCnt2)
                + (keyCnt2 > keyCnt1
                        ? (keyCnt2 - keyCnt1) * valCnt2
                        : (keyCnt1 - keyCnt2) * valCnt1);
    }

    @Override
    protected AbstractOuterJoinDriver<
                    Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>
            getOuterJoinDriver() {
        return new FullOuterJoinDriver<>();
    }
}
