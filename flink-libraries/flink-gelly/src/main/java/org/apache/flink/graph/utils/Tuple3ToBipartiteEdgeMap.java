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
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.bipartite.BipartiteEdge;

/**
 * Convert Tuple3 into a BipartiteEdge. First value in the tuple is used as the top vertex id, second value is used
 * as the bottom vertex id, and the third value is used as the edge value.
 *
 * @param <KT> the key type of top vertices
 * @param <KB> the key type of bottom vertices
 * @param <EV> the edge value type
 */
@FunctionAnnotation.ForwardedFields("f0; f1; f2")
public class Tuple3ToBipartiteEdgeMap<KT, KB, EV> implements MapFunction<Tuple3<KT, KB, EV>, BipartiteEdge<KT, KB, EV>> {

	private BipartiteEdge<KT, KB, EV> edge = new BipartiteEdge<>();

	@Override
	public BipartiteEdge<KT, KB, EV> map(Tuple3<KT, KB, EV> value) throws Exception {
		edge.setTopId(value.f0);
		edge.setBottomId(value.f1);
		edge.setValue(value.f2);
		return edge;
	}
}
