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
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sink that emits its input elements into a Cassandra database. This sink stores incoming records within a
 * {@link org.apache.flink.runtime.state.AbstractStateBackend}, and only commits them to cassandra
 * if a checkpoint is completed.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public class CassandraTupleWriteAheadSink<IN extends Tuple> extends GenericWriteAheadSink<IN> {
	protected transient Cluster cluster;
	protected transient Session session;

	private final String insertQuery;
	private transient PreparedStatement preparedStatement;

	private transient Throwable exception = null;
	private transient FutureCallback<ResultSet> callback;

	private ClusterBuilder builder;

	private int updatesSent = 0;
	private AtomicInteger updatesConfirmed = new AtomicInteger(0);

	private transient Object[] fields;

	protected CassandraTupleWriteAheadSink(String insertQuery, TypeSerializer<IN> serializer, ClusterBuilder builder, CheckpointCommitter committer) throws Exception {
		super(committer, serializer, UUID.randomUUID().toString().replace("-", "_"));
		this.insertQuery = insertQuery;
		this.builder = builder;
		ClosureCleaner.clean(builder, true);
	}

	public void open() throws Exception {
		super.open();
		if (!getRuntimeContext().isCheckpointingEnabled()) {
			throw new IllegalStateException("The write-ahead log requires checkpointing to be enabled.");
		}
		this.callback = new FutureCallback<ResultSet>() {
			@Override
			public void onSuccess(ResultSet resultSet) {
				updatesConfirmed.incrementAndGet();
			}

			@Override
			public void onFailure(Throwable throwable) {
				exception = throwable;
				LOG.error("Error while sending value.", throwable);
			}
		};
		cluster = builder.getCluster();
		session = cluster.connect();
		preparedStatement = session.prepare(insertQuery);

		fields = new Object[((TupleSerializer<IN>) serializer).getArity()];
	}

	@Override
	public void close() throws Exception {
		super.close();
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

	@Override
	protected void sendValues(Iterable<IN> values, long timestamp) throws Exception {
		//verify that no query failed until now
		if (exception != null) {
			throw new Exception(exception);
		}
		//set values for prepared statement
		for (IN value : values) {
			for (int x = 0; x < value.getArity(); x++) {
				fields[x] = value.getField(x);
			}
			//insert values and send to cassandra
			BoundStatement s = preparedStatement.bind(fields);
			s.setDefaultTimestamp(timestamp);
			ResultSetFuture result = session.executeAsync(s);
			updatesSent++;
			if (result != null) {
				//add callback to detect errors
				Futures.addCallback(result, callback);
			}
		}
		try {
			while (updatesSent != updatesConfirmed.get()) {
				Thread.sleep(100);
			}
		} catch (InterruptedException e) {
		}
		updatesSent = 0;
		updatesConfirmed.set(0);
	}
}
