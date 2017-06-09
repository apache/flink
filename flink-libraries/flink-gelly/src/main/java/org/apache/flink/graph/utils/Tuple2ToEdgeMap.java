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
import org.apache.flink.types.NullValue;

/**
 * Create an Edge from a Tuple2.
 *
 * <p>The new edge's value is set to {@link NullValue}.
 *
 * @param <K> edge ID type
 */
@ForwardedFields("f0; f1")
public class Tuple2ToEdgeMap<K> implements MapFunction<Tuple2<K, K>, Edge<K, NullValue>> {

	private static final long serialVersionUID = 1L;

	private Edge<K, NullValue> edge = new Edge<>(null, null, NullValue.getInstance());

	@Override
	public Edge<K, NullValue> map(Tuple2<K, K> tuple) {
		edge.f0 = tuple.f0;
		edge.f1 = tuple.f1;
		return edge;
	}

}
