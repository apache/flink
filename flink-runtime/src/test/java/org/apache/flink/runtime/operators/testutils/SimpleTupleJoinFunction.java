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

package org.apache.flink.runtime.operators.testutils;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

/** Simple flat join function that joins two binary tuples and considers null cases. */
public class SimpleTupleJoinFunction
        implements FlatJoinFunction<
                Tuple2<String, String>,
                Tuple2<String, Integer>,
                Tuple4<String, String, String, Object>> {

    @Override
    public void join(
            Tuple2<String, String> first,
            Tuple2<String, Integer> second,
            Collector<Tuple4<String, String, String, Object>> out)
            throws Exception {
        if (first == null) {
            out.collect(
                    new Tuple4<String, String, String, Object>(null, null, second.f0, second.f1));
        } else if (second == null) {
            out.collect(new Tuple4<String, String, String, Object>(first.f0, first.f1, null, null));
        } else {
            out.collect(
                    new Tuple4<String, String, String, Object>(
                            first.f0, first.f1, second.f0, second.f1));
        }
    }
}
