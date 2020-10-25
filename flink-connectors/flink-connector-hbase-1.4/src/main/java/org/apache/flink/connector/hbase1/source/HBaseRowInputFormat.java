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

package org.apache.flink.connector.hbase1.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.hbase.util.HBaseReadWriteHelper;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * {@link InputFormat} subclass that wraps the access for HTables. Returns the result as {@link Row}
 */
@Internal
public class HBaseRowInputFormat extends AbstractTableInputFormat<Row> implements ResultTypeQueryable<Row> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(HBaseRowInputFormat.class);

	private final String tableName;
	private final HBaseTableSchema schema;

	private transient HBaseReadWriteHelper readHelper;

	public HBaseRowInputFormat(org.apache.hadoop.conf.Configuration conf, String tableName, HBaseTableSchema schema) {
		super(conf);
		this.tableName = tableName;
		this.schema = schema;
	}

	@Override
	public void initTable() throws IOException {
		this.readHelper = new HBaseReadWriteHelper(schema);
		if (table == null) {
			connectToTable();
		}
		if (table != null && scan == null) {
			scan = getScanner();
		}
	}

	@Override
	protected Scan getScanner() {
		return readHelper.createScan();
	}

	@Override
	public String getTableName() {
		return tableName;
	}

	@Override
	protected Row mapResultToOutType(Result res) {
		return readHelper.parseToRow(res);
	}

	private void connectToTable() throws IOException {
		try {
			connection = ConnectionFactory.createConnection(getHadoopConfiguration());
			table = (HTable) connection.getTable(TableName.valueOf(tableName));
		} catch (TableNotFoundException tnfe) {
			LOG.error("The table " + tableName + " not found ", tnfe);
			throw new RuntimeException("HBase table '" + tableName + "' not found.", tnfe);
		}
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		// split the fieldNames
		String[] famNames = schema.getFamilyNames();
		TypeInformation<?>[] typeInfos = new TypeInformation[famNames.length];
		int i = 0;
		for (String family : famNames) {
			typeInfos[i] = new RowTypeInfo(
				schema.getQualifierTypes(family),
				schema.getQualifierNames(family));
			i++;
		}
		return new RowTypeInfo(typeInfos, famNames);
	}
}
