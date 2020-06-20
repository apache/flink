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

package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.util.Collector;

/**
 * The join runner to lookup the dimension table.
 */
public class LookupJoinRunner extends ProcessFunction<RowData, RowData> {
	private static final long serialVersionUID = -4521543015709964733L;

	private final GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher;
	private final GeneratedCollector<TableFunctionCollector<RowData>> generatedCollector;
	private final boolean isLeftOuterJoin;
	private final int tableFieldsCount;

	private transient FlatMapFunction<RowData, RowData> fetcher;
	protected transient TableFunctionCollector<RowData> collector;
	private transient GenericRowData nullRow;
	private transient JoinedRowData outRow;

	public LookupJoinRunner(
			GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher,
			GeneratedCollector<TableFunctionCollector<RowData>> generatedCollector,
			boolean isLeftOuterJoin,
			int tableFieldsCount) {
		this.generatedFetcher = generatedFetcher;
		this.generatedCollector = generatedCollector;
		this.isLeftOuterJoin = isLeftOuterJoin;
		this.tableFieldsCount = tableFieldsCount;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.fetcher = generatedFetcher.newInstance(getRuntimeContext().getUserCodeClassLoader());
		this.collector = generatedCollector.newInstance(getRuntimeContext().getUserCodeClassLoader());

		FunctionUtils.setFunctionRuntimeContext(fetcher, getRuntimeContext());
		FunctionUtils.setFunctionRuntimeContext(collector, getRuntimeContext());
		FunctionUtils.openFunction(fetcher, parameters);
		FunctionUtils.openFunction(collector, parameters);

		this.nullRow = new GenericRowData(tableFieldsCount);
		this.outRow = new JoinedRowData();
	}

	@Override
	public void processElement(RowData in, Context ctx, Collector<RowData> out) throws Exception {
		collector.setCollector(out);
		collector.setInput(in);
		collector.reset();

		// fetcher has copied the input field when object reuse is enabled
		fetcher.flatMap(in, getFetcherCollector());

		if (isLeftOuterJoin && !collector.isCollected()) {
			outRow.replace(in, nullRow);
			outRow.setRowKind(in.getRowKind());
			out.collect(outRow);
		}
	}

	public Collector<RowData> getFetcherCollector() {
		return collector;
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (fetcher != null) {
			FunctionUtils.closeFunction(fetcher);
		}
		if (collector != null) {
			FunctionUtils.closeFunction(collector);
		}
	}
}
