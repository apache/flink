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

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.Preconditions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * CassandraOutputFormatBase is the common abstract class for writing into Apache Cassandra.
 *
 * @param <OUT> Type of the elements to write.
 */
public abstract class CassandraOutputFormatBase<OUT> extends RichOutputFormat<OUT> {
	private static final Logger LOG = LoggerFactory.getLogger(CassandraOutputFormatBase.class);

	private final String insertQuery;
	private final ClusterBuilder builder;

	private transient Cluster cluster;
	private transient Session session;
	private transient PreparedStatement prepared;
	private transient FutureCallback<ResultSet> callback;
	private transient Throwable exception = null;

	public CassandraOutputFormatBase(String insertQuery, ClusterBuilder builder) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(insertQuery), "Query cannot be null or empty");
		Preconditions.checkArgument(builder != null, "Builder cannot be null");

		this.insertQuery = insertQuery;
		this.builder = builder;
	}

	@Override
	public void configure(Configuration parameters) {
		this.cluster = builder.getCluster();
	}

	/**
	 * Opens a Session to Cassandra and initializes the prepared statement.
	 *
	 * @param taskNumber The number of the parallel instance.
	 * @throws IOException Thrown, if the output could not be opened due to an
	 *                     I/O problem.
	 */
	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		this.session = cluster.connect();
		this.prepared = session.prepare(insertQuery);
		this.callback = new FutureCallback<ResultSet>() {
			@Override
			public void onSuccess(ResultSet ignored) {
				onWriteSuccess(ignored);
			}

			@Override
			public void onFailure(Throwable t) {
				onWriteFailure(t);
			}
		};
	}

	@Override
	public void writeRecord(OUT record) throws IOException {
		if (exception != null) {
			throw new IOException("write record failed", exception);
		}

		Object[] fields = extractFields(record);
		ResultSetFuture result = session.executeAsync(prepared.bind(fields));
		Futures.addCallback(result, callback);
	}

	protected abstract Object[] extractFields(OUT record);

	/**
	 * Callback that is invoked after a record is written to Cassandra successfully.
	 *
	 * <p>Subclass can override to provide its own logic.
	 * @param ignored the result.
	 */
	protected void onWriteSuccess(ResultSet ignored) {
	}

	/**
	 * Callback that is invoked when failing to write to Cassandra.
	 * Current implementation will record the exception and fail the job upon next record.
	 *
	 * <p>Subclass can override to provide its own failure handling logic.
	 * @param t the exception
	 */
	protected void onWriteFailure(Throwable t) {
		exception = t;
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
