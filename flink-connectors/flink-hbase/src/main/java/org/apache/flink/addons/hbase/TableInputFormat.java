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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;


/**
 * {@link InputFormat} subclass that wraps the access for HTables.
 */
public abstract class TableInputFormat<T extends Tuple> extends AbstractTableInputFormat<T> {

	private static final long serialVersionUID = 1L;

	/**
	 * Returns an instance of Scan that retrieves the required subset of records from the HBase table.
	 * @return The appropriate instance of Scan for this usecase.
	 */
	protected abstract Scan getScanner();

	/**
	 * What table is to be read.
	 * Per instance of a TableInputFormat derivative only a single tablename is possible.
	 * @return The name of the table
	 */
	protected abstract String getTableName();

	/**
	 * The output from HBase is always an instance of {@link Result}.
	 * This method is to copy the data in the Result instance into the required {@link Tuple}
	 * @param r The Result instance from HBase that needs to be converted
	 * @return The approriate instance of {@link Tuple} that contains the needed information.
	 */
	protected abstract T mapResultToTuple(Result r);

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
		table = createTable();
		if (table != null) {
			scan = getScanner();
		}
	}

	/**
	 * Create an {@link HTable} instance and set it into this format
	 */
	private HTable createTable() {
		LOG.info("Initializing HBaseConfiguration");
		//use files found in the classpath
		org.apache.hadoop.conf.Configuration hConf = HBaseConfiguration.create();

		try {
			return new HTable(hConf, getTableName());
		} catch (Exception e) {
			LOG.error("Error instantiating a new HTable instance", e);
		}
		return null;
	}

	protected T mapResultToOutType(Result r) {
		return mapResultToTuple(r);
	}
}
