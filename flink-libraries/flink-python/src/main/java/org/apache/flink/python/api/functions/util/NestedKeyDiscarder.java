/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.flink.python.api.functions.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Utility function to extract values from 2 Key-Value Tuples after a DefaultJoin.
 * @param <IN> input type
 */
@ForwardedFields("f0.f1->f0; f1.f1->f1")
public class NestedKeyDiscarder<IN> implements MapFunction<IN, Tuple2<byte[], byte[]>> {
	@Override
	@SuppressWarnings("unchecked")
	public Tuple2<byte[], byte[]> map(IN value) throws Exception {
		Tuple2<Tuple2<Tuple, byte[]>, Tuple2<Tuple, byte[]>> x = (Tuple2<Tuple2<Tuple, byte[]>, Tuple2<Tuple, byte[]>>) value;
		return new Tuple2<>(x.f0.f1, x.f1.f1);
	}
}
