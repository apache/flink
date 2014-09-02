/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.function.aggregation;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.types.TypeInformation;

public abstract class StreamingAggregationFunction<T> implements ReduceFunction<T> {
	private static final long serialVersionUID = 1L;
	
	protected int position;
	private TypeSerializer<Tuple> typeSerializer;
	protected Tuple returnTuple;

	public StreamingAggregationFunction(int pos) {
		this.position = pos;
	}

	@SuppressWarnings("unchecked")
	public void setType(TypeInformation<?> type) {
		this.typeSerializer = (TypeSerializer<Tuple>) type.createSerializer();
	}

	protected void copyTuple(Tuple tuple) throws InstantiationException, IllegalAccessException {
		returnTuple = (Tuple) typeSerializer.createInstance();
		typeSerializer.copy(tuple, returnTuple);
	}
}
