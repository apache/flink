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
import org.apache.flink.table.plan.nodes.datastream.aggs.DoubleSummaryAggregation;
import org.apache.flink.table.plan.nodes.datastream.aggs.IntegerSummaryAggregation;
import org.apache.flink.table.plan.nodes.datastream.aggs.LongSummaryAggregation;
import org.apache.flink.table.plan.nodes.datastream.aggs.StreamAggregator;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class DataStreamProcTimeAggregateRowAbstractWindowFunction implements Serializable{

	static final long serialVersionUID = 1L;
	List<String> aggregators;
	List<Integer> indexes;
	List<TypeInformation<?>> typeInfo;
	@SuppressWarnings("rawtypes")
	List<StreamAggregator> aggregatorImpl;

	public DataStreamProcTimeAggregateRowAbstractWindowFunction(List<String> aggregators, List<Integer> rowIndexes,
			List<TypeInformation<?>> typeInfos) {
		this.aggregators = aggregators;
		this.indexes = rowIndexes;
		this.typeInfo = typeInfos;
		aggregatorImpl = new ArrayList<>();
	}

	Row reuse;
	Row result;

	@SuppressWarnings("unchecked")
	protected void applyAggregation(Iterable<Row> input, Collector<Row> out) {

		for (Row row : input) {
			reuse = row;
			if (result == null) {
				result = new Row(reuse.getArity() + aggregators.size());
			}
			for (int i = 0; i < aggregators.size(); i++) {
				setAggregator(i, aggregators.get(i));
				aggregatorImpl.get(i).aggregate(reuse.getField(indexes.get(i)));
			}
		}

		for (int i = 0; i < reuse.getArity(); i++) {
			result.setField(i, reuse.getField(i));
		}
		for (int i = 0; i < aggregators.size(); i++) {
			result.setField(reuse.getArity() + i, aggregatorImpl.get(i).result());
			aggregatorImpl.get(i).reset();
		}
		out.collect(result);
	}

	protected void setAggregator(int i, String aggregatorName) {
		if (typeInfo.get(i).getTypeClass().equals(Integer.class)) {
			if (aggregatorImpl.size() - 1 < i) {
				aggregatorImpl.add(getIntegerAggregator(aggregatorName));
			}
		} else if (typeInfo.get(i).getTypeClass().equals(Double.class)) {
			if (aggregatorImpl.size() - 1 < i) {
				aggregatorImpl.add(getDoubleAggregator(aggregatorName));
			}
		} else if (typeInfo.get(i).getTypeClass().equals(Long.class)) {
			if (aggregatorImpl.size() - 1 < i) {
				aggregatorImpl.add(getLongAggregator(aggregatorName));
			}

		} else {
			throw new IllegalArgumentException("Unsupported aggregation type for " + aggregatorName
					+ "MAX, MIN, SUM, AVG, COUNT supported for Long, Double and Integer");
		}
	}

	protected StreamAggregator<?, ?> getIntegerAggregator(String aggregatorName) {
		StreamAggregator<?, ?> aggregator = null;
		if (aggregatorName.equals(SqlKind.MAX.toString())) {
			aggregator = new IntegerSummaryAggregation().initMax();
		} else if (aggregatorName.equals(SqlKind.MIN.toString())) {
			aggregator = new IntegerSummaryAggregation().initMin();
		} else if (aggregatorName.equals(SqlKind.SUM.toString()) || aggregatorName.equals(SqlKind.SUM0.toString())) {
			aggregator = new IntegerSummaryAggregation().initSum();
		} else if (aggregatorName.equals(SqlKind.AVG.toString())) {
			aggregator = new IntegerSummaryAggregation().initAvg();
		} else if (aggregatorName.equals(SqlKind.COUNT.toString())) {
			aggregator = new IntegerSummaryAggregation().initCount();
		} else {
			throw new IllegalArgumentException("Unsupported aggregation type of aggregation: " + aggregatorName
					+ ". Only MAX, MIN, SUM/SUM0, AVG, COUNT supported.");
		}
		return aggregator;
	}

	protected StreamAggregator<?, ?> getDoubleAggregator(String aggregatorName) {
		StreamAggregator<?, ?> aggregator = null;
		if (aggregatorName.equals(SqlKind.MAX.toString())) {
			aggregator = new DoubleSummaryAggregation().initMax();
		} else if (aggregatorName.equals(SqlKind.MIN.toString())) {
			aggregator = new DoubleSummaryAggregation().initMin();
		} else if (aggregatorName.equals(SqlKind.SUM.toString()) || aggregatorName.equals(SqlKind.SUM0.toString())) {
			aggregator = new DoubleSummaryAggregation().initSum();
		} else if (aggregatorName.equals(SqlKind.AVG.toString())) {
			aggregator = new DoubleSummaryAggregation().initAvg();
		} else if (aggregatorName.equals(SqlKind.COUNT.toString())) {
			aggregator = new DoubleSummaryAggregation().initCount();
		} else {
			throw new IllegalArgumentException("Unsupported aggregation type for " + aggregatorName
					+ ". Only MAX, MIN, SUM, AVG, COUNT supported.");
		}
		return aggregator;
	}

	protected StreamAggregator<?, ?> getLongAggregator(String aggregatorName) {
		StreamAggregator<?, ?> aggregator = null;
		if (aggregatorName.equals(SqlKind.MAX.toString())) {
			aggregator = new LongSummaryAggregation().initMax();
		} else if (aggregatorName.equals(SqlKind.MIN.toString())) {
			aggregator = new LongSummaryAggregation().initMin();
		} else if (aggregatorName.equals(SqlKind.SUM.toString()) || aggregatorName.equals(SqlKind.SUM0.toString())) {
			aggregator = new LongSummaryAggregation().initSum();
		} else if (aggregatorName.equals(SqlKind.AVG.toString())) {
			aggregator = new LongSummaryAggregation().initAvg();
		} else if (aggregatorName.equals(SqlKind.COUNT.toString())) {
			aggregator = new LongSummaryAggregation().initCount();
		} else {
			throw new IllegalArgumentException(
					"Unsupported aggregation type for " + aggregatorName + ". Only Integer, Double, Long supported.");
		}
		return aggregator;
	}

}
