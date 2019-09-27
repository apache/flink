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
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.util.Collector;

/**
 * The join runner to lookup the dimension table.
 */
public class LookupJoinRunner extends ProcessFunction<BaseRow, BaseRow> {
	private static final long serialVersionUID = -4521543015709964733L;

	private final GeneratedFunction<FlatMapFunction<BaseRow, BaseRow>> generatedFetcher;
	private final GeneratedCollector<TableFunctionCollector<BaseRow>> generatedCollector;
	private final boolean isLeftOuterJoin;
	private final int tableFieldsCount;

	private transient FlatMapFunction<BaseRow, BaseRow> fetcher;
	protected transient TableFunctionCollector<BaseRow> collector;
	private transient GenericRow nullRow;
	private transient JoinedRow outRow;

	public LookupJoinRunner(
			GeneratedFunction<FlatMapFunction<BaseRow, BaseRow>> generatedFetcher,
			GeneratedCollector<TableFunctionCollector<BaseRow>> generatedCollector,
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

		this.nullRow = new GenericRow(tableFieldsCount);
		this.outRow = new JoinedRow();
	}

	@Override
	public void processElement(BaseRow in, Context ctx, Collector<BaseRow> out) throws Exception {
		collector.setCollector(out);
		collector.setInput(in);
		collector.reset();

		// fetcher has copied the input field when object reuse is enabled
		fetcher.flatMap(in, getFetcherCollector());

		if (isLeftOuterJoin && !collector.isCollected()) {
			outRow.replace(in, nullRow);
			outRow.setHeader(in.getHeader());
			out.collect(outRow);
		}
	}

	public Collector<BaseRow> getFetcherCollector() {
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
