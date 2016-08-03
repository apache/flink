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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * A client class that serves to create connection and send data to HBase.
 */
class HBaseClient implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(HBaseClient.class);

	private Connection connection;
	private Table table;

	public HBaseClient(org.apache.hadoop.conf.Configuration hbConfig, String tableName) throws IOException {
		connection = ConnectionFactory.createConnection(hbConfig);
		table = connection.getTable(TableName.valueOf(tableName));
	}

	public void sendDataToHbase(List<Mutation> mutations) throws IOException, InterruptedException {
		Object[] results = new Object[mutations.size()];
		table.batch(mutations, results);
	}

	@Override
	public void close() throws IOException {
		try {
			if (table != null) {
				table.close();
			}
		} catch (Exception e) {
			LOG.error("Error while closing HBase table.", e);
		}
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (Exception e) {
			LOG.error("Error while closing HBase connection.", e);
		}
	}
}
