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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;

/**
 * create an Edge DataSetfrom a Tuple3 dataset
 *
 * @param <K>
 * @param <EV>
 */
public class Tuple3ToEdgeMap<K, EV> implements MapFunction<Tuple3<K, K, EV>, Edge<K, EV>> {

	private static final long serialVersionUID = 1L;

	public Edge<K, EV> map(Tuple3<K, K, EV> tuple) {
		return new Edge<K, EV>(tuple.f0, tuple.f1, tuple.f2);
	}

}
