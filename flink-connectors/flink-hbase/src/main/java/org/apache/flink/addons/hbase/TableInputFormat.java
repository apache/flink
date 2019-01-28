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

package org.apache.flink.addons.hbase;

import org.apache.flink.addons.hbase.strategy.TableInputSplitStrategyImpl;
import org.apache.flink.addons.hbase.util.HBaseConnectorUtil;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

/**
 * {@link InputFormat} subclass that wraps the access for HTables.
 */
public abstract class TableInputFormat<T extends Tuple> extends AbstractTableInputFormat<T>
	implements HBaseTableScannerAware, ResultToTupleMapper<T> {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new table input format.
	 */
	public TableInputFormat() {
		tableInputSplitStrategy = new TableInputSplitStrategyImpl();
	}

	/**
	 * Creates a {@link Scan} object and opens the {@link HTable} connection.
	 * These are opened here because they are needed in the createInputSplits
	 * which is called before the openInputFormat method.
	 * So the connection is opened in {@link #configure(Configuration)} and closed in {@link #closeInputFormat()}.
	 *
	 * @param parameters The configuration that is to be used
	 * @see Configuration
	 */
	@Override
	public void configure(Configuration parameters) {
		table = HBaseConnectorUtil.createTable(getTableName(), null);
		super.configure(parameters);
	}

	@Override
	protected T mapResultToOutType(Result r) {
		return mapResultToTuple(r);
	}
}
