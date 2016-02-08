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

import java.io.File;
import java.io.Serializable;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandle;

import static java.util.Objects.requireNonNull;

/**
 *
 */
public class RocksDBStateBackend extends AbstractStateBackend {
	private static final long serialVersionUID = 1L;

	/** Base path for RocksDB directory. */
	private final String dbBasePath;

	/** The checkpoint directory that we snapshot RocksDB backups to. */
	private final String checkpointDirectory;

	/** Operator identifier that is used to uniqueify the RocksDB storage path. */
	private String operatorIdentifier;

	/** JobID for uniquifying backup paths. */
	private JobID jobId;

	private AbstractStateBackend backingStateBackend;

	public RocksDBStateBackend(String dbBasePath, String checkpointDirectory, AbstractStateBackend backingStateBackend) {
		this.dbBasePath = requireNonNull(dbBasePath);
		this.checkpointDirectory = requireNonNull(checkpointDirectory);
		this.backingStateBackend = requireNonNull(backingStateBackend);
	}

	@Override
	public void initializeForJob(Environment env,
		String operatorIdentifier,
		TypeSerializer<?> keySerializer) throws Exception {
		super.initializeForJob(env, operatorIdentifier, keySerializer);
		this.operatorIdentifier = operatorIdentifier.replace(" ", "");
		backingStateBackend.initializeForJob(env, operatorIdentifier, keySerializer);
		this.jobId = env.getJobID();
	}

	@Override
	public void disposeAllStateForCurrentJob() throws Exception {

	}

	@Override
	public void close() throws Exception {

	}

	private File getDbPath(String stateName) {
		return new File(new File(new File(new File(dbBasePath), jobId.toString()), operatorIdentifier), stateName);
	}

	private String getCheckpointPath(String stateName) {
		return checkpointDirectory + "/" + jobId.toString() + "/" + operatorIdentifier + "/" + stateName;
	}

	@Override
	protected <N, T> ValueState<T> createValueState(TypeSerializer<N> namespaceSerializer,
		ValueStateDescriptor<T> stateDesc) throws Exception {
		File dbPath = getDbPath(stateDesc.getName());
		String checkpointPath = getCheckpointPath(stateDesc.getName());
		return new RocksDBValueState<>(keySerializer, namespaceSerializer, stateDesc, dbPath, checkpointPath);
	}

	@Override
	protected <N, T> ListState<T> createListState(TypeSerializer<N> namespaceSerializer,
		ListStateDescriptor<T> stateDesc) throws Exception {
		File dbPath = getDbPath(stateDesc.getName());
		String checkpointPath = getCheckpointPath(stateDesc.getName());
		return new RocksDBListState<>(keySerializer, namespaceSerializer, stateDesc, dbPath, checkpointPath);
	}

	@Override
	protected <N, T> ReducingState<T> createReducingState(TypeSerializer<N> namespaceSerializer,
		ReducingStateDescriptor<T> stateDesc) throws Exception {
		File dbPath = getDbPath(stateDesc.getName());
		String checkpointPath = getCheckpointPath(stateDesc.getName());
		return new RocksDBReducingState<>(keySerializer, namespaceSerializer, stateDesc, dbPath, checkpointPath);
	}

	@Override
	public CheckpointStateOutputStream createCheckpointStateOutputStream(long checkpointID,
		long timestamp) throws Exception {
		return backingStateBackend.createCheckpointStateOutputStream(checkpointID, timestamp);
	}

	@Override
	public <S extends Serializable> StateHandle<S> checkpointStateSerializable(S state,
		long checkpointID,
		long timestamp) throws Exception {
		return backingStateBackend.checkpointStateSerializable(state, checkpointID, timestamp);
	}
}
