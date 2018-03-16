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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;

/**
 * Create an Edge DataSet from a Tuple3 DataSet.
 *
 * @param <K> edge ID type
 * @param <EV> edge value type
 */
@ForwardedFields("f0; f1; f2")
public class Tuple3ToEdgeMap<K, EV> implements MapFunction<Tuple3<K, K, EV>, Edge<K, EV>> {

	private static final long serialVersionUID = 1L;

	private Edge<K, EV> edge = new Edge<>();

	@Override
	public Edge<K, EV> map(Tuple3<K, K, EV> tuple) {
		edge.f0 = tuple.f0;
		edge.f1 = tuple.f1;
		edge.f2 = tuple.f2;
		return edge;
	}

}
