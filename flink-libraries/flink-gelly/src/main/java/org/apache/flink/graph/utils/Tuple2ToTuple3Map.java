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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.NullValue;

/**
 * Mapper function to convert a Tuple2 instance into a Tuple3 instance. Fields 0 and 1 are copied to the the
 * result tuple, while field 3 is assigned to null.
 *
 * @param <K1> type of the the field 0 in input and output tuples.
 * @param <K2> type of the the field 1 in input and output tuples.
 */
@FunctionAnnotation.ForwardedFields("f0; f1")
public class Tuple2ToTuple3Map<K1, K2> implements MapFunction<Tuple2<K1, K2>, Tuple3<K1, K2, NullValue>> {

	private static final long serialVersionUID = -2981792951286476970L;

	private Tuple3<K1, K2, NullValue> result = new Tuple3<>();

	public Tuple3<K1, K2, NullValue> map(Tuple2<K1, K2> edge) {
		result.f0 = edge.f0;
		result.f1 = edge.f1;
		return result;
	}
}
