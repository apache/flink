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

package org.apache.flink.graph.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;

/**
 * Create a Tuple2 DataSet from the vertices of an Edge DataSet.
 *
 * @param <K> edge ID type
 * @param <EV> edge value type
 */
@ForwardedFields("f0; f1")
public class EdgeToTuple2Map<K, EV> implements MapFunction<Edge<K, EV>, Tuple2<K, K>> {

    private static final long serialVersionUID = 1L;

    private Tuple2<K, K> output = new Tuple2<>();

    @Override
    public Tuple2<K, K> map(Edge<K, EV> edge) {
        output.f0 = edge.f0;
        output.f1 = edge.f1;
        return output;
    }
}
