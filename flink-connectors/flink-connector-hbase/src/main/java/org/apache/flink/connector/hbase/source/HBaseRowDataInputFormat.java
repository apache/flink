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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.util.HBaseSerde;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.table.data.RowData;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * {@link InputFormat} subclass that wraps the access for HTables. Returns the result as {@link RowData}
 */
public class HBaseRowDataInputFormat extends AbstractTableInputFormat<RowData> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(HBaseRowDataInputFormat.class);

	private final String tableName;
	private final HBaseTableSchema schema;
	private final String nullStringLiteral;

	private transient HBaseSerde serde;

	public HBaseRowDataInputFormat(
			org.apache.hadoop.conf.Configuration conf,
			String tableName,
			HBaseTableSchema schema,
			String nullStringLiteral) {
		super(conf);
		this.tableName = tableName;
		this.schema = schema;
		this.nullStringLiteral = nullStringLiteral;
	}

	@Override
	public void configure(Configuration parameters) {
		LOG.info("Initializing HBase configuration.");
		this.serde = new HBaseSerde(schema, nullStringLiteral);
		connectToTable();
		if (table != null) {
			scan = getScanner();
		}
	}

	@Override
	protected Scan getScanner() {
		return serde.createScan();
	}

	@Override
	public String getTableName() {
		return tableName;
	}

	@Override
	protected RowData mapResultToOutType(Result res) {
		return serde.convertToRow(res);
	}

	private void connectToTable() {
		try {
			Connection conn = ConnectionFactory.createConnection(getHadoopConfiguration());
			super.table = (HTable) conn.getTable(TableName.valueOf(tableName));
		} catch (TableNotFoundException tnfe) {
			LOG.error("The table " + tableName + " not found ", tnfe);
			throw new RuntimeException("HBase table '" + tableName + "' not found.", tnfe);
		} catch (IOException ioe) {
			LOG.error("Exception while creating connection to HBase.", ioe);
			throw new RuntimeException("Cannot create connection to HBase.", ioe);
		}
	}
}
