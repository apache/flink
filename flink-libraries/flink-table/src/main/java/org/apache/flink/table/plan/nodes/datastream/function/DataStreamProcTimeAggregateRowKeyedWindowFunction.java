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

import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class DataStreamProcTimeAggregateRowKeyedWindowFunction
		extends DataStreamProcTimeAggregateRowAbstractWindowFunction
		implements WindowFunction<Row, Row, Tuple, GlobalWindow> {

	static final long serialVersionUID = 1L;

	public DataStreamProcTimeAggregateRowKeyedWindowFunction(List<String> aggregators, List<Integer> rowIndexes,
			List<TypeInformation<?>> typeInfos) {
		super(aggregators, rowIndexes, typeInfos);
	}

	@Override
	public void apply(Tuple key, GlobalWindow window, Iterable<Row> input, Collector<Row> out) throws Exception {
		super.applyAggregation(input, out);
	}

}
