/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.GenericListState;
import org.apache.flink.runtime.state.GenericReducingState;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.contrib.streaming.state.SQLRetrier.retry;

/**
 * {@link AbstractStateBackend} for storing checkpoints in JDBC supporting databases.
 * Key-Value state is stored out-of-core and is lazily fetched using the
 * {@link LazyDbValueState} implementation. A different backend can also be
 * provided in the constructor to store the non-partitioned states. A common use
 * case would be to store the key-value states in the database and store larger
 * non-partitioned states on a distributed file system.
 * <p>
 * This backend implementation also allows the sharding of the checkpointed
 * states among multiple database instances, which can be enabled by passing
 * multiple database urls to the {@link DbBackendConfig} instance.
 * <p>
 * By default there are multiple tables created in the given databases: 1 table
 * for non-partitioned checkpoints and 1 table for each key-value state in the
 * streaming program.
 * <p>
 * To control table creation, insert/lookup operations and to provide
 * compatibility for different SQL implementations, a custom
 * {@link MySqlAdapter} can be supplied in the {@link DbBackendConfig}.
 *
 */
public class DbStateBackend extends AbstractStateBackend {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(DbStateBackend.class);

	private Random rnd;

	// ------------------------------------------------------

	private transient Environment env;

	// ------------------------------------------------------

	private final DbBackendConfig dbConfig;
	private final DbAdapter dbAdapter;

	private ShardedConnection connections;

	private final int numSqlRetries;
	private final int sqlRetrySleep;

	private transient PreparedStatement insertStatement;

	private String operatorIdentifier;

	// ------------------------------------------------------

	// We allow to use a different backend for storing non-partitioned states
	private AbstractStateBackend nonPartitionedStateBackend = null;

	// ------------------------------------------------------

	/**
	 * Create a new {@link DbStateBackend} using the provided
	 * {@link DbBackendConfig} configuration.
	 * 
	 */
	public DbStateBackend(DbBackendConfig backendConfig) {
		this.dbConfig = backendConfig;
		dbAdapter = backendConfig.getDbAdapter();
		numSqlRetries = backendConfig.getMaxNumberOfSqlRetries();
		sqlRetrySleep = backendConfig.getSleepBetweenSqlRetries();
	}

	/**
	 * Create a new {@link DbStateBackend} using the provided
	 * {@link DbBackendConfig} configuration and a different backend for storing
	 * non-partitioned state snapshots.
	 * 
	 */
	public DbStateBackend(DbBackendConfig backendConfig, AbstractStateBackend backend) {
		this(backendConfig);
		this.nonPartitionedStateBackend = backend;
	}

	/**
	 * Get the database connections maintained by the backend.
	 */
	public ShardedConnection getConnections() {
		return connections;
	}

	/**
	 * Check whether the backend has been initialized.
	 * 
	 */
	public boolean isInitialized() {
		return connections != null;
	}

	public Environment getEnvironment() {
		return env;
	}

	/**
	 * Get the backend configuration object.
	 */
	public DbBackendConfig getConfiguration() {
		return dbConfig;
	}

	@Override
	public <S extends Serializable> StateHandle<S> checkpointStateSerializable(final S state, final long checkpointID,
			final long timestamp) throws Exception {

		// If we set a different backend for non-partitioned checkpoints we use
		// that otherwise write to the database.
		if (nonPartitionedStateBackend == null) {
			return retry(new Callable<DbStateHandle<S>>() {
				public DbStateHandle<S> call() throws Exception {
					// We create a unique long id for each handle, but we also
					// store the checkpoint id and timestamp for bookkeeping
					long handleId = rnd.nextLong();

					// We use the ApplicationID here, because it is restored when
					// the job is started from a savepoint (whereas the job ID
					// changes with each submission).
					String appIdShort = env.getApplicationID().toShortString();

					byte[] serializedState = InstantiationUtil.serializeObject(state);
					dbAdapter.setCheckpointInsertParams(appIdShort, insertStatement,
							checkpointID, timestamp, handleId,
							serializedState);

					insertStatement.executeUpdate();

					return new DbStateHandle<>(appIdShort, checkpointID, timestamp, handleId,
							dbConfig, serializedState.length);
				}
			}, numSqlRetries, sqlRetrySleep);
		} else {
			return nonPartitionedStateBackend.checkpointStateSerializable(state, checkpointID, timestamp);
		}
	}

	@Override
	public CheckpointStateOutputStream createCheckpointStateOutputStream(long checkpointID, long timestamp)
			throws Exception {
		if (nonPartitionedStateBackend == null) {
			// We don't implement this functionality for the DbStateBackend as
			// we cannot directly write a stream to the database anyways.
			throw new UnsupportedOperationException("Use ceckpointStateSerializable instead.");
		} else {
			return nonPartitionedStateBackend.createCheckpointStateOutputStream(checkpointID, timestamp);
		}
	}

	@Override
	protected <N, T> ValueState<T> createValueState(TypeSerializer<N> namespaceSerializer,
		ValueStateDescriptor<T> stateDesc) throws Exception {
		String stateName = operatorIdentifier + "_"+ stateDesc.getName();

		return new LazyDbValueState<>(
			stateName,
			env.getTaskInfo().getIndexOfThisSubtask() == 0,
			getConnections(),
			getConfiguration(),
			keySerializer,
			namespaceSerializer,
			stateDesc);
	}

	@Override
	protected <N, T> ListState<T> createListState(TypeSerializer<N> namespaceSerializer,
		ListStateDescriptor<T> stateDesc) throws Exception {
		ValueStateDescriptor<ArrayList<T>> valueStateDescriptor = new ValueStateDescriptor<>(stateDesc.getName(), null, new ArrayListSerializer<>(stateDesc.getSerializer()));
		ValueState<ArrayList<T>> valueState = createValueState(namespaceSerializer, valueStateDescriptor);
		return new GenericListState<>(valueState);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected <N, T> ReducingState<T> createReducingState(TypeSerializer<N> namespaceSerializer,
		ReducingStateDescriptor<T> stateDesc) throws Exception {

		ValueStateDescriptor<T> valueStateDescriptor = new ValueStateDescriptor<>(stateDesc.getName(), null, stateDesc.getSerializer());
		ValueState<T> valueState = createValueState(namespaceSerializer, valueStateDescriptor);
		return new GenericReducingState<>(valueState, stateDesc.getReduceFunction());
	}

	@Override
	public void initializeForJob(final Environment env,
		String operatorIdentifier,
		TypeSerializer<?> keySerializer) throws Exception {
		super.initializeForJob(env, operatorIdentifier, keySerializer);

		this.operatorIdentifier = operatorIdentifier;

		this.rnd = new Random();
		this.env = env;

		connections = dbConfig.createShardedConnection();

		// We want the most light-weight transaction isolation level as we don't
		// have conflicting reads/writes. We just want to be able to roll back
		// batch inserts for k-v snapshots. This requirement might be removed in
		// the future.
		connections.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

		// If we have a different backend for non-partitioned states we
		// initialize that, otherwise create tables for storing the checkpoints.
		//
		// Currently all non-partitioned states are written to the first
		// database shard
		if (nonPartitionedStateBackend == null) {
			insertStatement = retry(new Callable<PreparedStatement>() {
				public PreparedStatement call() throws SQLException {
					dbAdapter.createCheckpointsTable(env.getApplicationID().toShortString(), getConnections().getFirst());
					return dbAdapter.prepareCheckpointInsert(env.getApplicationID().toShortString(),
							getConnections().getFirst());
				}
			}, numSqlRetries, sqlRetrySleep);
		} else {
			nonPartitionedStateBackend.initializeForJob(env, operatorIdentifier, keySerializer);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Database state backend successfully initialized");
		}
	}

	@Override
	public void close() throws Exception {
		// We first close the statement/non-partitioned backend, then we close
		// the database connection
		try (ShardedConnection c = connections) {
			if (nonPartitionedStateBackend == null) {
				insertStatement.close();
			} else {
				nonPartitionedStateBackend.close();
			}
		}
	}

	@Override
	public void disposeAllStateForCurrentJob() throws Exception {
		if (nonPartitionedStateBackend == null) {
			dbAdapter.disposeAllStateForJob(env.getApplicationID().toShortString(), connections.getFirst());
		} else {
			nonPartitionedStateBackend.disposeAllStateForCurrentJob();
		}
	}
}
