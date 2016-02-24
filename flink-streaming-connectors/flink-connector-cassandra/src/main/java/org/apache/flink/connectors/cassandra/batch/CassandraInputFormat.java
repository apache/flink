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

package org.apache.flink.connectors.cassandra.batch;

import java.io.IOException;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.connectors.cassandra.streaming.ClusterConfigurator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 	InputFormat to read data from Apache Cassandra and generate ${@link Tuple}.
 *
 * @param <OUT> type of Tuple
 */
public abstract class CassandraInputFormat<OUT extends Tuple> extends
		RichInputFormat<OUT, InputSplit> implements NonParallelInput,
		ClusterConfigurator {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraInputFormat.class);
	private static final long serialVersionUID = 1L;
	
	private final String query;
	private transient Cluster cluster;
	private transient Session session;
	private transient ResultSet rs;

	public CassandraInputFormat(String query) {
		if(Strings.isNullOrEmpty(query)){
			throw new IllegalArgumentException("Query cannot be null or empty");
		}
		this.query = query;
	}

	@Override
	public void configure(Configuration parameters) {
		this.cluster = configureCluster(Cluster.builder()).build();
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics)
			throws IOException {
		return cachedStatistics;
	}

	/**
	 * Opens a Session and executes the query.
	 *
	 * @param ignored
	 * @throws IOException
	 */
	@Override
	public void open(InputSplit ignored) throws IOException {
		this.session = cluster.connect();
		this.rs = session.execute(query);
	}

	@Override
	public boolean reachedEnd() throws IOException {
		final Boolean res = rs.isExhausted();
		if (res) {
			this.close();
		}
		return res;
	}

	@Override
	public OUT nextRecord(OUT reuse) throws IOException {
		final Row item = rs.one();

		for (int i = 0; i < reuse.getArity(); i++) {
			reuse.setField(item.getObject(i), i);
		}
		
		return reuse;
	}

	@Override
	public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
		GenericInputSplit[] split = { new GenericInputSplit(0, 1) };
		return split;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	/**
	 * Closes all resources used.
	 */
	@Override
	public void close() throws IOException {
		try {
			session.close();
		}catch(Exception e) {
			LOG.info("Inputformat couldn't be closed - " + e.getMessage());
		}

		try {
			cluster.close();
		} catch(Exception e) {
			LOG.info("Inputformat couldn't be closed - " + e.getMessage());
		}
	}
}