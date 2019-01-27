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

package org.apache.flink.connectors.hbase.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connectors.hbase.util.HBaseBytesSerializer;
import org.apache.flink.connectors.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.connectors.hbase.util.HBaseTypeUtils;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * The HBaseLookupFunction is a standard user-defined table function, it can be used in tableAPI
 * and also useful for temporal table join plan in SQL.
 */
public class HBaseLookupFunction extends TableFunction<Row> {

	private static final long serialVersionUID = 6552257501168286801L;
	private static final Logger LOG = LoggerFactory.getLogger(HBaseLookupFunction.class);

	private final RichTableSchema tableSchema; // for type info
	private final HBaseTableSchemaV2 hTableSchema;
	// rowkey field index in source input row
	private final int rowKeySourceIndex;
	// qualifier fields' indexes in source input row
	private final List<Integer> qualifierSourceIndexes;
	private final String hTableName;

	private List<Tuple3<byte[], byte[], TypeInformation<?>>> qualifierList;
	// field serializer for HBase format, order by field index of input row
	private List<HBaseBytesSerializer> inputFieldSerializers;
	// TODO currently this rowKey always be a binary type (adapt to sql-level type system), should refactor later
	private int rowKeyInternalTypeIndex;
	private int totalQualifiers;
	private String charset;
	private byte[] serializedConfig;

	private transient Connection hConnection;
	protected transient HTable table;

	public HBaseLookupFunction(
			RichTableSchema tableSchema,
			String hbaseTableName,
			HBaseTableSchemaV2 hbaseSchema,
			int rowKeySourceIndex,
			List<Integer> qualifierSourceIndexes,
			Configuration hbaseConfigurations) throws IOException {

		this.tableSchema = tableSchema;
		this.hTableName = hbaseTableName;
		this.hTableSchema = hbaseSchema;
		// Configuration is not serializable
		this.serializedConfig = HBaseConfigurationUtil.serializeConfiguration(hbaseConfigurations);

		this.totalQualifiers = hbaseSchema.getFamilySchema().getTotalQualifiers();
		Preconditions.checkArgument(null != qualifierSourceIndexes && totalQualifiers == qualifierSourceIndexes.size(),
			"qualifierSourceIndexes info should be consistent with qualifiers defined in HBaseTableSchema!");
		Preconditions.checkArgument(rowKeySourceIndex > -1 && totalQualifiers > rowKeySourceIndex,
			"rowKeySourceIndex must > -1 and totalQualifiers number must > rowKeySourceIndex");

		this.qualifierList = hbaseSchema.getFamilySchema().getFlatByteQualifiers();
		this.rowKeySourceIndex = rowKeySourceIndex;
		this.qualifierSourceIndexes = qualifierSourceIndexes;
		this.charset = hbaseSchema.getFamilySchema().getStringCharset();
		this.inputFieldSerializers = new ArrayList<>();
		this.rowKeyInternalTypeIndex = HBaseTypeUtils.getTypeIndex(hbaseSchema.getRowKeyType());

		for (int index = 0; index <= totalQualifiers; index++) {
			if (index == rowKeySourceIndex) {
				inputFieldSerializers.add(new HBaseBytesSerializer(hbaseSchema.getRowKeyType(), charset));
			} else {
				Tuple3<byte[], byte[], TypeInformation<?>> typeInfo;
				if (index < rowKeySourceIndex) {
					typeInfo = qualifierList.get(index);
				} else {
					typeInfo = qualifierList.get(index - 1);
				}
				inputFieldSerializers.add(new HBaseBytesSerializer(typeInfo.f2, charset));
			}
		}
	}

	@Override
	public DataType getResultType(Object[] arguments, Class[] argTypes) {
		return tableSchema.getResultRowType();
	}

	protected Get createGet(Object rowKey) throws IOException {
		// currently this rowKey always be a binary type (adapt to sql-level type system)
		byte[] rowkey = HBaseTypeUtils.serializeFromInternalObject(rowKey, rowKeyInternalTypeIndex, HBaseTypeUtils.DEFAULT_CHARSET);
		Get get = new Get(rowkey);
		get.setMaxVersions(1);
		// add request colums
		for (Tuple3<byte[], byte[], TypeInformation<?>> typeInfo : qualifierList) {
			get.addColumn(typeInfo.f0, typeInfo.f1);
		}
		return get;
	}

	public void eval(Object rowKey) throws IOException {
		// fetch result
		Result result = table.get(createGet(rowKey));
		if (!result.isEmpty()) {
			// parse and collect
			collect(parseResult(result, rowKey, rowKeySourceIndex));
		}
	}

	protected Row parseResult(Result result, Object rowKey, int rowKeySourceIndex) throws UnsupportedEncodingException {
		// output rowKey + qualifiers
		Row row = new Row(totalQualifiers + 1);
		row.setField(rowKeySourceIndex, rowKey);
		for (int idx = 0; idx < totalQualifiers; idx++) {
			Tuple3<byte[], byte[], TypeInformation<?>> qInfo = qualifierList.get(idx);
			byte[] value = result.getValue(qInfo.f0, qInfo.f1);
			int qualifierSrcIdx = qualifierSourceIndexes.get(idx);
			row.setField(qualifierSrcIdx, inputFieldSerializers.get(qualifierSrcIdx).fromHBaseBytes(value));
		}
		return row;
	}

	protected org.apache.hadoop.conf.Configuration prepareRuntimeConfiguration() throws IOException {
		// create default configuration from current runtime env (`hbase-site.xml` in classpath) first,
		// and overwrite configuration using serialized configuration from client-side env (`hbase-site.xml` in classpath).
		// user params from client-side have the highest priority
		org.apache.hadoop.conf.Configuration runtimeConfig = HBaseConfigurationUtil.deserializeConfiguration(serializedConfig, HBaseConfiguration.create());

		// do validation: check key option(s) in final runtime configuration
		if (StringUtils.isNullOrWhitespaceOnly(runtimeConfig.get(HConstants.ZOOKEEPER_QUORUM))) {
			LOG.error(String.format("can not connect to hbase without {%s} configuration"), HConstants.ZOOKEEPER_QUORUM);
			throw new IOException("check hbase configuration failed, lost: '" + HConstants.ZOOKEEPER_QUORUM + "'!");
		}

		return runtimeConfig;
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		LOG.info("start open ...");
		org.apache.hadoop.conf.Configuration config = prepareRuntimeConfiguration();
		try {
			hConnection = ConnectionFactory.createConnection(config);
			table = (HTable) hConnection.getTable(TableName.valueOf(hTableName));
		} catch (TableNotFoundException tnfe) {
			LOG.error("Table '{}' not found ", hTableName, tnfe);
			throw new RuntimeException("HBase table '" + hTableName + "' not found.", tnfe);
		} catch (IOException ioe) {
			LOG.error("Exception while creating connection to HBase.", ioe);
			throw new RuntimeException("Cannot create connection to HBase.", ioe);
		}
		LOG.info("end open.");
	}

	@Override
	public void close() {
		LOG.info("start close ...");
		if (null != table) {
			try {
				table.close();
			} catch (IOException e) {
				// ignore exception when close.
				LOG.warn("exception when close table", e);
			}
		}
		if (null != hConnection) {
			try {
				hConnection.close();
			} catch (IOException e) {
				// ignore exception when close.
				LOG.warn("exception when close connection", e);
			}
		}
		LOG.info("end close.");
	}
}
