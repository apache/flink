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

package org.apache.flink.table.plan.nodes.datastream.function;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class DataStreamProcTimeAggregateWindowFunction extends DataStreamProcTimeWindowAggregator
		implements WindowFunction<Row, Row, Tuple, GlobalWindow> {

	public DataStreamProcTimeAggregateWindowFunction(List<String> aggregators, List<Integer> indexes,
			List<TypeInformation<?>> typeOutput, List<TypeInformation<?>> typeInput) {
		this.aggregators = aggregators;
		this.indexes = indexes;
		this.typeOutput = typeOutput;
		this.typeInput = typeInput;
		aggregatorImpl = new ArrayList<>();

		for (int i = 0; i < aggregators.size(); i++) {
			setAggregator(i, aggregators.get(i));
		}

	}

	@Override
	public void apply(Tuple key, GlobalWindow window, Iterable<Row> input, Collector<Row> out) throws Exception {

		Row lastElement = null;
		Row result = null;
		for (int i = 0; i < aggregators.size(); i++) {
			aggregatorImpl.get(i).reset();
		}

		for (Row rowObj : input) {
			lastElement = rowObj;

			for (int i = 0; i < aggregators.size(); i++) {
				aggregatorImpl.get(i).aggregate(lastElement.getField(indexes.get(i)));
			}
		}

		// conversion rules for windows typically insert the elements of the
		// object as well.
		// a verification for number of expected fields to be the sum of input
		// arity + number of aggregators can be done
		result = new Row(lastElement.getArity() + aggregators.size());
		for (int i = 0; lastElement != null && i < lastElement.getArity(); i++) {
			result.setField(i, lastElement.getField(i));
		}

		for (int i = 0; i < aggregators.size(); i++) {
			result.setField(lastElement.getArity() + i, aggregatorImpl.get(i).result());
		}

		out.collect(result);

	}

}
