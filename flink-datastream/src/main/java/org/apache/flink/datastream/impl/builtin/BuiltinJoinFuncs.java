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

package org.apache.flink.datastream.impl.builtin;

import org.apache.flink.datastream.api.builtin.BuiltinFuncs;
import org.apache.flink.datastream.api.extension.join.JoinFunction;
import org.apache.flink.datastream.api.extension.join.JoinType;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.join.operators.TwoInputNonBroadcastJoinProcessFunction;

/** Join-related implementations in {@link BuiltinFuncs}. */
public class BuiltinJoinFuncs {

    /**
     * Wrap the JoinFunction and JoinType within a ProcessFunction to perform the Join operation.
     * Note that the wrapped process function should only be used with KeyedStream.
     */
    public static <IN1, IN2, OUT> TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> join(
            JoinFunction<IN1, IN2, OUT> joinFunction, JoinType joinType) {
        return new TwoInputNonBroadcastJoinProcessFunction<>(joinFunction, joinType);
    }
}
