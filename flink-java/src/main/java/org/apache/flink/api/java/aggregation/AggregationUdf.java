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

package org.apache.flink.api.java.aggregation;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;

/**
 * UDf passed to {@link GroupReduceFunction} to compute a list of
 * {@link AggregationFunction}'s.
 * 
 * @param <T> Input type of {@code reduce}. 
 * @param <R> Output type of {@code reduce}.
 */
public class AggregationUdf<T, R extends Tuple> implements GroupReduceFunction<T, R>, Serializable {
	private static final long serialVersionUID = 5563658873455921533L;

	private AggregationFunction<?, ?>[] functions;
	private R result;
	
	public AggregationUdf(R result, AggregationFunction<?, ?>... functions) {
		this.result = result;
		this.functions = functions;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void reduce(Iterable<T> records, Collector<R> out) throws Exception {
		for (AggregationFunction<?, ?> function : functions) {
			function.initialize();
		}
		
		Tuple current = null;
		Iterator<T> values = records.iterator();
		
		// TODO extract key() functions from list and call them only once
		
		while (values.hasNext()) {
			current = (Tuple) values.next();
			for (AggregationFunction function : functions) {
				int fieldPosition = function.getFieldPosition();
				Object fieldValue = current.getField(fieldPosition);
				function.aggregate(fieldValue);
			}
		}
		
		int index = 0;
		for (AggregationFunction<?, ?> function : functions) {
			Object aggregatedValue = function.getAggregate();
			result.setField(aggregatedValue, index);
			index += 1;
		}
		out.collect(result);
	}
}