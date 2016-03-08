/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericAtLeastOnceSink;

/**
 * Sink that emits its input elements into a Cassandra database. This sink is integrated with the checkpointing
 * mechanism and provides exactly-once guarantees for idempotent updates.
 * <p/>
 * Incoming records are stored within a {@link org.apache.flink.runtime.state.AbstractStateBackend}, and only committed if a
 * checkpoint is completed.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public class CassandraIdempotentExactlyOnceSink<IN extends Tuple> extends GenericAtLeastOnceSink<IN> {
	protected transient Cluster cluster;
	protected transient Session session;

	private final String insertQuery;
	private transient PreparedStatement preparedStatement;

	private transient Throwable exception = null;
	private transient FutureCallback<ResultSet> callback;

	private ClusterBuilder builder;

	protected CassandraIdempotentExactlyOnceSink(String insertQuery, TypeSerializer<IN> serializer, ClusterBuilder builder, String jobID, CheckpointCommitter committer) throws Exception {
		super(committer, serializer, jobID);
		this.insertQuery = insertQuery;
		this.builder = builder;
		ClosureCleaner.clean(builder, true);
	}

	public void open() throws Exception {
		super.open();
		this.callback = new FutureCallback<ResultSet>() {
			@Override
			public void onSuccess(ResultSet resultSet) {
			}

			@Override
			public void onFailure(Throwable throwable) {
				exception = throwable;
			}
		};
		cluster = builder.getCluster();
		session = cluster.connect();
		preparedStatement = session.prepare(insertQuery);
	}

	@Override
	public void close() throws Exception {
		super.close();
		try {
			session.close();
		} catch (Exception e) {
			LOG.error("Error while closing session.", e);
		}
		try {
			cluster.close();
		} catch (Exception e) {
			LOG.error("Error while closing cluster.", e);
		}
	}

	@Override
	protected void sendValue(Iterable<IN> values, long timestamp) throws Exception {
		//verify that no query failed until now
		if (exception != null) {
			throw new Exception(exception);
		}
		//set values for prepared statement
		for (IN value : values) {
			Object[] fields = new Object[value.getArity()];
			for (int x = 0; x < value.getArity(); x++) {
				fields[x] = value.getField(x);
			}
			//insert values and send to cassandra
			BoundStatement s = preparedStatement.bind(fields);
			s.setDefaultTimestamp(timestamp);
			ResultSetFuture result = session.executeAsync(s);
			if (result != null) {
				//add callback to detect errors
				Futures.addCallback(result, callback);
			}
		}
	}
}
