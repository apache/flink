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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * A sink that writes its input to an HBase table.
 * To create this sink you need to pass two arguments the name of the HBase table and {@link HBaseMapper}.
 * A boolean field writeToWAL can also be set to enable or disable Write Ahead Log (WAL).
 * HBase config files must be located in the classpath to create a connection to HBase cluster.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public class HBaseSink<IN> extends RichSinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(HBaseSink.class);

	private transient HBaseClient client;
	private String tableName;
	private HBaseMapper<IN> mapper;
	private boolean writeToWAL = true;

	/**
	 * The main constructor for creating HBaseSink.
	 *
	 * @param tableName the name of the HBase table
	 * @param mapper the mapping of input a value to a HBase row
	 */
	public HBaseSink(String tableName, HBaseMapper<IN> mapper) {
		Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "Table name cannot be null or empty.");
		Preconditions.checkArgument(mapper != null, "HBase mapper cannot be null.");
		this.tableName = tableName;
		this.mapper = mapper;
	}

	/**
	 * Enable or disable WAL when writing to HBase.
	 * Set to true (default) if you want to write {@link Mutation}s to the WAL synchronously.
	 * Set to false if you do not want to write {@link Mutation}s to the WAL.
	 *
	 * @param writeToWAL
	 * @return the HBaseSink with specified writeToWAL value
	 */
	public HBaseSink writeToWAL(boolean writeToWAL) {
		this.writeToWAL = writeToWAL;
		return this;
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		try {
			client = new HBaseClient(HBaseConfiguration.create(), tableName);
		} catch (IOException e) {
			LOG.error("HBase sink preparation failed.", e);
			throw e;
		}
	}

	@Override
	public void invoke(IN value) throws Exception {
		byte[] rowKey = mapper.rowKey(value);
		MutationActions actions = mapper.actions(value);
		List<Mutation> mutations = actions.createMutations(rowKey, writeToWAL);
		if (!mutations.isEmpty()) {
			try {
				client.sendDataToHbase(mutations);
			} catch (Exception e) {
				LOG.error("Error while sending data to HBase.", e);
				throw e;
			}
		}
	}

	@Override
	public void close() throws Exception {
		client.close();
	}
}
