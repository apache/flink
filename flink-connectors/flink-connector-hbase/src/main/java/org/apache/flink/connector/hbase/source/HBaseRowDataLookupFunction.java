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

package org.apache.flink.connector.hbase.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.connector.hbase.util.HBaseSerde;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The HBaseRowDataLookupFunction is a standard user-defined table function, it can be used in tableAPI
 * and also useful for temporal table join plan in SQL. It looks up the result as {@link RowData}.
 */
@Internal
public class HBaseRowDataLookupFunction extends TableFunction<RowData> {

	private static final Logger LOG = LoggerFactory.getLogger(HBaseRowDataLookupFunction.class);
	private static final long serialVersionUID = 1L;

	private final String hTableName;
	private final byte[] serializedConfig;
	private final HBaseTableSchema hbaseTableSchema;
	private final String nullStringLiteral;

	private transient Connection hConnection;
	private transient Table table;
	private transient HBaseSerde serde;

	public HBaseRowDataLookupFunction(
			Configuration configuration,
			String hTableName,
			HBaseTableSchema hbaseTableSchema,
			String nullStringLiteral) {
		this.serializedConfig = HBaseConfigurationUtil.serializeConfiguration(configuration);
		this.hTableName = hTableName;
		this.hbaseTableSchema = hbaseTableSchema;
		this.nullStringLiteral = nullStringLiteral;
	}

	/**
	 * The invoke entry point of lookup function.
	 * @param rowKey the lookup key. Currently only support single rowkey.
	 */
	public void eval(Object rowKey) throws IOException {
		// fetch result
		Get get = serde.createGet(rowKey);
		if (get != null) {
			Result result = table.get(get);
			if (!result.isEmpty()) {
				// parse and collect
				collect(serde.convertToRow(result));
			}
		}
	}

	private Configuration prepareRuntimeConfiguration() {
		// create default configuration from current runtime env (`hbase-site.xml` in classpath) first,
		// and overwrite configuration using serialized configuration from client-side env (`hbase-site.xml` in classpath).
		// user params from client-side have the highest priority
		Configuration runtimeConfig = HBaseConfigurationUtil.deserializeConfiguration(
			serializedConfig,
			HBaseConfigurationUtil.getHBaseConfiguration());

		// do validation: check key option(s) in final runtime configuration
		if (StringUtils.isNullOrWhitespaceOnly(runtimeConfig.get(HConstants.ZOOKEEPER_QUORUM))) {
			LOG.error("can not connect to HBase without {} configuration", HConstants.ZOOKEEPER_QUORUM);
			throw new IllegalArgumentException("check HBase configuration failed, lost: '" + HConstants.ZOOKEEPER_QUORUM + "'!");
		}

		return runtimeConfig;
	}

	@Override
	public void open(FunctionContext context) {
		LOG.info("start open ...");
		Configuration config = prepareRuntimeConfiguration();
		try {
			hConnection = ConnectionFactory.createConnection(config);
			table = (Table) hConnection.getTable(TableName.valueOf(hTableName));
		} catch (TableNotFoundException tnfe) {
			LOG.error("Table '{}' not found ", hTableName, tnfe);
			throw new RuntimeException("HBase table '" + hTableName + "' not found.", tnfe);
		} catch (IOException ioe) {
			LOG.error("Exception while creating connection to HBase.", ioe);
			throw new RuntimeException("Cannot create connection to HBase.", ioe);
		}
		this.serde = new HBaseSerde(hbaseTableSchema, nullStringLiteral);
		LOG.info("end open.");
	}

	@Override
	public void close() {
		LOG.info("start close ...");
		if (null != table) {
			try {
				table.close();
				table = null;
			} catch (IOException e) {
				// ignore exception when close.
				LOG.warn("exception when close table", e);
			}
		}
		if (null != hConnection) {
			try {
				hConnection.close();
				hConnection = null;
			} catch (IOException e) {
				// ignore exception when close.
				LOG.warn("exception when close connection", e);
			}
		}
		LOG.info("end close.");
	}

	@VisibleForTesting
	public String getHTableName() {
		return hTableName;
	}
}
