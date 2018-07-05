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

package org.apache.flink.batch.connectors.cassandra;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * InputFormat to read data from Apache Cassandra and generate a custom Cassandra annotated object.
 * @param <OUT> type of inputClass
 */
public class CassandraPojoInputFormat<OUT> extends RichInputFormat<OUT, InputSplit> implements NonParallelInput {
	private static final Logger LOG = LoggerFactory.getLogger(CassandraInputFormat.class);

	private final String query;
	private final ClusterBuilder builder;

	private transient Cluster cluster;
	private transient Session session;
	private transient Result<OUT> resultSet;
	private Class<OUT> inputClass;

	public CassandraPojoInputFormat(String query, ClusterBuilder builder, Class<OUT> inputClass) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(query), "Query cannot be null or empty");
		Preconditions.checkArgument(builder != null, "Builder cannot be null");
		Preconditions.checkArgument(inputClass != null, "InputClass cannot be null");

		this.query = query;
		this.builder = builder;
		this.inputClass = inputClass;
	}

	@Override
	public void configure(Configuration parameters) {
		this.cluster = builder.getCluster();
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
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
		MappingManager manager = new MappingManager(session);

		Mapper<OUT> mapper = manager.mapper(inputClass);

		this.resultSet = mapper.map(session.execute(query));
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return resultSet.isExhausted();
	}

	@Override
	public OUT nextRecord(OUT reuse) throws IOException {
		return resultSet.one();
	}

	@Override
	public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
		GenericInputSplit[] split = {new GenericInputSplit(0, 1)};
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
			if (session != null) {
				session.close();
			}
		} catch (Exception e) {
			LOG.error("Error while closing session.", e);
		}

		try {
			if (cluster != null) {
				cluster.close();
			}
		} catch (Exception e) {
			LOG.error("Error while closing cluster.", e);
		}
	}
}
