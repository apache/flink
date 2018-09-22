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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for {@link RichInputFormat} to read data from Apache Cassandra and generate a custom Cassandra annotated object.
 *
 * @param <OUT> type of inputClass
 */
public abstract class CassandraInputFormatBase<OUT> extends RichInputFormat<OUT, InputSplit> implements NonParallelInput {
	private static final long serialVersionUID = -1519372881115104601L;
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	protected final String query;
	private final ClusterBuilder builder;

	protected transient Cluster cluster;
	protected transient Session session;

	public CassandraInputFormatBase(String query, ClusterBuilder builder){
		Preconditions.checkArgument(!Strings.isNullOrEmpty(query), "Query cannot be null or empty");
		Preconditions.checkNotNull(builder, "Builder cannot be null");

		this.query = query;
		this.builder = builder;
	}

	@Override
	public void configure(Configuration parameters) {
		this.cluster = builder.getCluster();
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		return cachedStatistics;
	}

	@Override
	public InputSplit[] createInputSplits(int minNumSplits) {
		return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	/**
	 * Closes all resources used.
	 */
	@Override
	public void close() {
		try {
			if (session != null) {
				session.close();
			}
		} catch (Exception e) {
			logger.error("Error while closing session.", e);
		}

		try {
			if (cluster != null) {
				cluster.close();
			}
		} catch (Exception e) {
			logger.error("Error while closing cluster.", e);
		}
	}

}
