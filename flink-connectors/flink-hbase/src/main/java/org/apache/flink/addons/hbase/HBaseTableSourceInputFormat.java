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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link InputFormat} subclass that wraps the access for HTables. Returns the result as {@link Row}
 */
public class HBaseTableSourceInputFormat extends TableInputFormat<Row> implements ResultTypeQueryable<Row> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(HBaseTableSourceInputFormat.class);
	private String tableName;
	private transient Connection conn;
	private transient org.apache.hadoop.conf.Configuration conf;
	private HBaseTableSchema schema;

	public HBaseTableSourceInputFormat(org.apache.hadoop.conf.Configuration conf, String tableName, HBaseTableSchema schema) {
		this.tableName = tableName;
		this.conf = conf;
		this.schema = schema;
	}

	@Override
	public void configure(Configuration parameters) {
		LOG.info("Initializing HBaseConfiguration");
		connectToTable();
		if(table != null) {
			scan = getScanner();
		}
	}

	@Override
	protected Scan getScanner() {
		Scan scan = new Scan();
		for(String family : schema.getFamilyNames()) {
			for(String qualifierName : schema.getQualifierNames(family)) {
				scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifierName));
			}
		}
		return scan;
	}

	@Override
	public String getTableName() {
		return tableName;
	}

	@Override
	protected Row mapResultToTuple(Result res) {
		List<Object> values = new ArrayList<Object>();
		int i = 0;
		String[] familyNames = schema.getFamilyNames();
		Object[] rows = new Object[familyNames.length];
		for(String family : familyNames) {
			for(Pair<String, TypeInformation<?>> info : schema.getFamilyInfo(family)) {
				byte[] value = res.getValue(Bytes.toBytes(family), Bytes.toBytes(info.getFirst()));
				if(value != null) {
					values.add(schema.deserialize(value, info.getSecond()));
				} else {
					values.add(null);
				}
			}
			rows[i] = Row.of(values.toArray(new Object[values.size()]));
			values.clear();
			i++;
		}
		return Row.of(rows);
	}

	private void connectToTable() {
		//use files found in the classpath
		if(this.conf == null) {
			this.conf = HBaseConfiguration.create();
		}
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch(IOException ioe) {
			LOG.error("Exception while creating connection to hbase cluster", ioe);
			return;
		}
		try {
			table = (HTable)conn.getTable(TableName.valueOf(tableName));
		} catch(TableNotFoundException tnfe) {
			LOG.error("The table " + tableName + " not found ", tnfe);
		} catch(IOException ioe) {
			LOG.error("Exception while connecting to the table "+tableName+ " ", ioe);
		}
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		// split the fieldNames
		String[] famNames = schema.getFamilyNames();
		TypeInformation<?>[] typeInfos = new TypeInformation[famNames.length];
		int i = 0;
		for (String family : famNames) {
			typeInfos[i] = new RowTypeInfo(schema.getTypeInformation(family), schema.getQualifierNames(family));
			i++;
		}
		RowTypeInfo rowInfo = new RowTypeInfo(typeInfos, famNames);
		return rowInfo;
	}
}
