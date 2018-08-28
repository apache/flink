/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is used to configure a {@link Connection} and a {@link Table} after deployment.
 * The connection represents the connection that will be established to HBase.
 * The table represents a table can be manipulated in the hbase.
 */
public class HBaseTableBuilder implements Serializable {

	// Configuration is not Serializable
	private Map<String, String> configurationMap = new HashMap<>();

	private String tableName;
	private boolean bufferEnabled = false;
	private boolean clusterKeyConfigured = false;

	public HBaseTableBuilder withClusterKey(String clusterKey) throws IOException {
		mergeClusterkeyToConfiguration(clusterKey);
		clusterKeyConfigured = true;
		return this;
	}

	public HBaseTableBuilder withTableName(String tableName) {
		this.tableName = tableName;
		return this;
	}

	public HBaseTableBuilder enableBuffer(boolean bufferEnabled) {
		this.bufferEnabled = bufferEnabled;
		return this;
	}

	public HBaseTableBuilder addProperty(String key, String value) {
		configurationMap.put(key, value);
		return this;
	}

	public boolean isClusterKeyConfigured() {
		return clusterKeyConfigured;
	}

	public String getTableName() {
		return tableName;
	}

	public Connection buildConnection() throws IOException {
		Configuration configuration = new Configuration();
		for (String key : configurationMap.keySet()) {
			configuration.set(key, configurationMap.get(key));
		}
		return ConnectionFactory.createConnection(configuration);
	}

	public Table buildTable(Connection connection) throws IOException {
		Table hTable = connection.getTable(TableName.valueOf(tableName));

		if (hTable instanceof HTable && bufferEnabled) {
			((HTable) hTable).setAutoFlush(false, false);
		}
		Admin admin = connection.getAdmin();
		try {
			if (!admin.isTableAvailable(TableName.valueOf(this.tableName))) {
				throw new IOException("Table is not available.");
			}
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}
			} catch (Throwable t) {

			}
		}
		return hTable;
	}

	private void mergeClusterkeyToConfiguration(String clusterKey)
		throws IOException {
		if (clusterKey == null) {
			throw new IOException("ClusterKey is null.");
		}
		String[] segments = clusterKey.split(":");
		if (segments.length > 3) {
			throw new IOException("ClusterKey:[" + clusterKey + "] is illegal.");
		}
		if (segments.length > 0) {
			configurationMap.put(HConstants.ZOOKEEPER_QUORUM, segments[0]);
		}
		if (segments.length > 1) {
			configurationMap.put(HConstants.ZOOKEEPER_CLIENT_PORT, segments[1]);
		}
		if (segments.length > 2) {
			configurationMap.put(HConstants.ZOOKEEPER_ZNODE_PARENT, segments[2]);
		}
	}
}
