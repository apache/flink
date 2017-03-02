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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlKind;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.Accumulator;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.aggfunctions.CountAggFunction;
import org.apache.flink.table.functions.aggfunctions.DoubleAvgAggFunction;
import org.apache.flink.table.functions.aggfunctions.DoubleMaxAggFunction;
import org.apache.flink.table.functions.aggfunctions.DoubleMinAggFunction;
import org.apache.flink.table.functions.aggfunctions.DoubleSumAggFunction;
import org.apache.flink.table.functions.aggfunctions.IntAvgAggFunction;
import org.apache.flink.table.functions.aggfunctions.IntMaxAggFunction;
import org.apache.flink.table.functions.aggfunctions.IntMinAggFunction;
import org.apache.flink.table.functions.aggfunctions.IntSumAggFunction;
import org.apache.flink.table.functions.aggfunctions.LongAvgAggFunction;
import org.apache.flink.table.functions.aggfunctions.LongMaxAggFunction;
import org.apache.flink.table.functions.aggfunctions.LongMinAggFunction;
import org.apache.flink.table.functions.aggfunctions.LongSumAggFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class DataStreamProcTimeAggregateRowAbstractWindowFunction implements Serializable {

	static final long serialVersionUID = 1L;
	List<String> aggregators;
	List<Integer> indexes;
	List<TypeInformation<?>> typeInfo;
	@SuppressWarnings("rawtypes")
	List<AggregateFunction> aggregatorImpl;
	List<Accumulator> accumulators;

	public DataStreamProcTimeAggregateRowAbstractWindowFunction(List<String> aggregators, List<Integer> rowIndexes,
			List<TypeInformation<?>> typeInfos) {
		this.aggregators = aggregators;
		this.indexes = rowIndexes;
		this.typeInfo = typeInfos;
		aggregatorImpl = new ArrayList<>();
		accumulators = new ArrayList<>();
		for (int i = 0; i < aggregators.size(); i++) {
			setAggregator(i, aggregators.get(i));
		}
	}

	Row reuse;
	Row result;

	protected void applyAggregation(Iterable<Row> input, Collector<Row> out) {

		for (Row row : input) {
			reuse = row;
			if (result == null) {
				result = new Row(reuse.getArity() + aggregators.size());
			}
			for (int i = 0; i < aggregators.size(); i++) {
				aggregatorImpl.get(i).accumulate(accumulators.get(i), reuse.getField(indexes.get(i)));
			}
		}

		for (int i = 0; i < reuse.getArity(); i++) {
			result.setField(i, reuse.getField(i));
		}
		for (int i = 0; i < aggregators.size(); i++) {
			result.setField(reuse.getArity() + i, aggregatorImpl.get(i).getValue(accumulators.get(i)));
			accumulators.get(i).reset();
		}
		out.collect(result);
	}

	protected void setAggregator(int i, String aggregatorName) {
		if (typeInfo.get(i).getTypeClass().equals(Integer.class)) {
			aggregatorImpl.add(getIntegerAggregator(aggregatorName));
		} else if (typeInfo.get(i).getTypeClass().equals(Double.class)) {
			aggregatorImpl.add(getDoubleAggregator(aggregatorName));
		} else if (typeInfo.get(i).getTypeClass().equals(Long.class)) {
			aggregatorImpl.add(getLongAggregator(aggregatorName));
		} else {
			throw new IllegalArgumentException("Unsupported aggregation type for " + aggregatorName
					+ "MAX, MIN, SUM, AVG, COUNT supported for Long, Double and Integer");
		}
		accumulators.add(aggregatorImpl.get(i).createAccumulator());
	}

	protected AggregateFunction<?> getIntegerAggregator(String aggregatorName) {
		AggregateFunction<?> aggregator = null;
		if (aggregatorName.equals(SqlKind.MAX.toString())) {
			aggregator = new IntMaxAggFunction();
		} else if (aggregatorName.equals(SqlKind.MIN.toString())) {
			aggregator = new IntMinAggFunction();
		} else if (aggregatorName.equals(SqlKind.SUM.toString()) || aggregatorName.equals(SqlKind.SUM0.toString())) {
			aggregator = new IntSumAggFunction();
		} else if (aggregatorName.equals(SqlKind.AVG.toString())) {
			aggregator = new IntAvgAggFunction();
		} else if (aggregatorName.equals(SqlKind.COUNT.toString())) {
			aggregator = new CountAggFunction();
		} else {
			throw new IllegalArgumentException("Unsupported aggregation type of aggregation: " + aggregatorName
					+ ". Only MAX, MIN, SUM/SUM0, AVG, COUNT supported.");
		}
		return aggregator;
	}

	protected AggregateFunction<?> getDoubleAggregator(String aggregatorName) {
		AggregateFunction<?> aggregator = null;
		if (aggregatorName.equals(SqlKind.MAX.toString())) {
			aggregator = new DoubleMaxAggFunction();
		} else if (aggregatorName.equals(SqlKind.MIN.toString())) {
			aggregator = new DoubleMinAggFunction();
		} else if (aggregatorName.equals(SqlKind.SUM.toString()) || aggregatorName.equals(SqlKind.SUM0.toString())) {
			aggregator = new DoubleSumAggFunction();
		} else if (aggregatorName.equals(SqlKind.AVG.toString())) {
			aggregator = new DoubleAvgAggFunction();
		} else if (aggregatorName.equals(SqlKind.COUNT.toString())) {
			aggregator = new CountAggFunction();
		} else {
			throw new IllegalArgumentException("Unsupported aggregation type for " + aggregatorName
					+ ". Only MAX, MIN, SUM, AVG, COUNT supported.");
		}
		return aggregator;
	}

	protected AggregateFunction<?> getLongAggregator(String aggregatorName) {
		AggregateFunction<?> aggregator = null;
		if (aggregatorName.equals(SqlKind.MAX.toString())) {
			aggregator = new LongMaxAggFunction();
		} else if (aggregatorName.equals(SqlKind.MIN.toString())) {
			aggregator = new LongMinAggFunction();
		} else if (aggregatorName.equals(SqlKind.SUM.toString()) || aggregatorName.equals(SqlKind.SUM0.toString())) {
			aggregator = new LongSumAggFunction();
		} else if (aggregatorName.equals(SqlKind.AVG.toString())) {
			aggregator = new LongAvgAggFunction();
		} else if (aggregatorName.equals(SqlKind.COUNT.toString())) {
			aggregator = new CountAggFunction();
		} else {
			throw new IllegalArgumentException(
					"Unsupported aggregation type for " + aggregatorName + ". Only Integer, Double, Long supported.");
		}
		return aggregator;
	}

}
