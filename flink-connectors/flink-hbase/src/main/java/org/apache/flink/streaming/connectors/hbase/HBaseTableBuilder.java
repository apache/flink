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

import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
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

	private Map<String, String> configurationMap = new HashMap<>();

	private String tableName;

	public static HBaseTableBuilder builder() {
		return new HBaseTableBuilder();
	}

	public HBaseTableBuilder zkQuorum(String zkQuorum) {
		configurationMap.put(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
		return this;
	}

	public HBaseTableBuilder zkClientPort(int port) {
		configurationMap.put(HConstants.ZOOKEEPER_CLIENT_PORT, String.valueOf(port));
		return this;
	}

	public HBaseTableBuilder zkPath(String zkPath) {
		configurationMap.put(HConstants.ZOOKEEPER_ZNODE_PARENT, zkPath);
		return this;
	}

	public HBaseTableBuilder addProperty(String key, String value) {
		configurationMap.put(key, value);
		return this;
	}

	public HBaseTableBuilder addProperties(Map<String, String> properties) {
		configurationMap.putAll(properties);
		return this;
	}

	public HBaseTableBuilder tableName(String tableName) {
		this.tableName = tableName;
		return this;
	}

	public Connection buildConnection() throws IOException {
		Preconditions.checkNotNull(configurationMap.get(HConstants.ZOOKEEPER_QUORUM),
			"Zookeeper quorum must be configured for HBase sink.");
		Preconditions.checkNotNull(configurationMap.get(HConstants.ZOOKEEPER_CLIENT_PORT),
			"Zookeeper client port must be configured for HBase sink.");
		Preconditions.checkNotNull(configurationMap.get(HConstants.ZOOKEEPER_ZNODE_PARENT),
			"Zookeeper path must be configured for HBase sink.");

		Configuration configuration = new Configuration();
		for (Map.Entry<String, String> entry: configurationMap.entrySet()) {
			configuration.set(entry.getKey(), entry.getValue());
		}
		return ConnectionFactory.createConnection(configuration);
	}

	public Table buildTable(Connection connection) throws IOException {
		Preconditions.checkNotNull(connection, "No connection created for HBase sink.");
		Preconditions.checkNotNull(tableName, "Tablename must be configured for HBase sink");
		try (Table table = connection.getTable(TableName.valueOf(tableName));
			Admin admin = connection.getAdmin()) {
			if (!admin.isTableAvailable(TableName.valueOf(this.tableName))) {
				throw new IOException("Table is not available.");
			}
			return table;
		}
	}
}
