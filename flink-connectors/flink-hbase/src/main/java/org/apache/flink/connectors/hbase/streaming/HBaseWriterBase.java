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

package org.apache.flink.connectors.hbase.streaming;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.hbase.table.HBaseTableSchemaV2;
import org.apache.flink.connectors.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A base stream writer for HBase engine, commonly an sub-class implementation should override `invoke` method for sink function.
 * An example:
 *     @Override
 *     public void invoke(T value, Context context) throws Exception {
 *        // convert T to HBase Mutation(Put/Delete)
 *        ...
 *
 *        // then call hbase-client's api to send the mutation request to (remote) region server.
 *        htable.put/delete ...
 *
 *        // or you can do some writing optimization such as batchPut / asyncIO
 *        // thus, maybe you should implements {@link org.apache.flink.streaming.api.checkpoint.CheckpointedFunction}
 *        // and do some thing before `snapshotState`
 *     }
 * Configuration apply sequence:
 * User constructed configuration from client > `hbase-site.xml` in client classpath > `hbase-site.xml` in runtime classpath.
 */
public abstract class HBaseWriterBase<T> extends RichSinkFunction<T> {

	private static final long serialVersionUID = 6237464944162580678L;
	private static final Logger LOG = LoggerFactory.getLogger(HBaseWriterBase.class);

	protected String hTableName;
	protected HBaseTableSchemaV2 hTableSchema;
	private byte[] serializedConfig;
	private transient Connection hConnection;
	protected transient HTable table;

	public HBaseWriterBase(String hTableName, HBaseTableSchemaV2 hTableSchema) throws IOException {
		// serialize default HBaseConfiguration from client's env
		this(hTableName, hTableSchema, HBaseConfiguration.create());
	}

	public HBaseWriterBase(String hTableName, HBaseTableSchemaV2 hTableSchema, org.apache.hadoop.conf.Configuration conf) throws IOException {
		this.hTableName = hTableName;
		this.hTableSchema = hTableSchema;
		// Configuration is not serializable
		this.serializedConfig = HBaseConfigurationUtil.serializeConfiguration(conf);
	}

	private org.apache.hadoop.conf.Configuration prepareRuntimeConfiguration() throws IOException {
		// create default configuration from current runtime env (`hbase-site.xml` in classpath) first,
		// and overwrite configuration using serialized configuration from client-side env (`hbase-site.xml` in classpath).
		// user params from client-side have the highest priority
		org.apache.hadoop.conf.Configuration runtimeConfig = HBaseConfigurationUtil.deserializeConfiguration(serializedConfig, HBaseConfiguration.create());

		// do validation: check key option(s) in final runtime configuration
		if (StringUtils.isNullOrWhitespaceOnly(runtimeConfig.get(HConstants.ZOOKEEPER_QUORUM))) {
			LOG.error(String.format("can not connect to hbase without {%s} configuration", HConstants.ZOOKEEPER_QUORUM));
			throw new IOException("check hbase configuration failed, lost: '" + HConstants.ZOOKEEPER_QUORUM + "'!");
		}

		return runtimeConfig;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		LOG.info("start open ...");
		org.apache.hadoop.conf.Configuration config = prepareRuntimeConfiguration();
		try {
			if (null == hConnection) {
				hConnection = ConnectionFactory.createConnection(config);
			}
			table = (HTable) hConnection.getTable(TableName.valueOf(hTableName));
		} catch (TableNotFoundException tnfe) {
			LOG.error("The table " + hTableName + " not found ", tnfe);
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
