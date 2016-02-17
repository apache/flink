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
package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.io.disk.InputViewIterator;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.util.NonReusingMutableToRegularIteratorWrapper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Generic Sink that emits its input elements into an arbitrary backend. This sink is integrated with the checkpointing
 * mechanism to provide near at-least-once semantics.
 * <p/>
 * Incoming records are stored within a {@link org.apache.flink.runtime.state.AbstractStateBackend}, and only committed if a
 * checkpoint is completed. Should a job fail, while data is being committed, data will be committed twice.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public abstract class GenericAtLeastOnceSink<IN> extends AbstractStreamOperator<IN> implements OneInputStreamOperator<IN, IN> {
	protected static final Logger LOG = LoggerFactory.getLogger(GenericAtLeastOnceSink.class);
	private transient AbstractStateBackend.CheckpointStateOutputView out;
	protected final TypeSerializer<IN> serializer;
	protected final CheckpointCommitter committer;
	protected final String id;

	private ExactlyOnceState state = new ExactlyOnceState();

	public GenericAtLeastOnceSink(CheckpointCommitter committer, TypeSerializer<IN> serializer) {
		if (committer == null) {
			throw new IllegalArgumentException("CheckpointCommitter argument must not be null.");
		}
		this.committer = committer;
		this.serializer = serializer;
		this.id = UUID.randomUUID().toString();
	}

	@Override
	public void open() throws Exception {
		committer.setOperatorId(id);
		committer.setOperatorSubtaskId(getRuntimeContext().getIndexOfThisSubtask());
		committer.open();
	}

	public void close() throws Exception {
		committer.close();
	}

	/**
	 * Saves a handle in the state.
	 *
	 * @param checkpointId
	 * @throws IOException
	 */
	private void saveHandleInState(final long checkpointId) throws IOException {
		//only add handle if a new OperatorState was created since the last snapshot
		if (out != null) {
			StateHandle<DataInputView> handle = out.closeAndGetHandle();
			state.pendingHandles.put(checkpointId, handle);
			out = null;
		}
	}

	@Override
	public StreamTaskState snapshotOperatorState(final long checkpointId, final long timestamp) throws Exception {
		StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);
		saveHandleInState(checkpointId);
		taskState.setFunctionState(state);
		return taskState;
	}

	@Override
	public void restoreState(StreamTaskState state, long recoveryTimestamp) throws Exception {
		super.restoreState(state, recoveryTimestamp);
		this.state = (ExactlyOnceState) state.getFunctionState();
		out = null;
	}

	@Override
	public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
		super.notifyOfCompletedCheckpoint(checkpointId);

		synchronized (state.pendingHandles) {
			Set<Long> pastCheckpointIds = state.pendingHandles.keySet();
			Set<Long> checkpointsToRemove = new HashSet<>();
			for (Long pastCheckpointId : pastCheckpointIds) {
				if (pastCheckpointId <= checkpointId) {
					if (!committer.isCheckpointCommitted(pastCheckpointId)) {
						StateHandle<DataInputView> handle = state.pendingHandles.get(pastCheckpointId);
						DataInputView in = handle.getState(getUserCodeClassloader());
						sendValue(new NonReusingMutableToRegularIteratorWrapper<>(new InputViewIterator<>(in, serializer), serializer));
						committer.commitCheckpoint(pastCheckpointId);
					}
					checkpointsToRemove.add(pastCheckpointId);
				}
			}
			for (Long toRemove : checkpointsToRemove) {
				StateHandle<DataInputView> handle = state.pendingHandles.get(toRemove);
				state.pendingHandles.remove(toRemove);
				handle.discardState();
			}
		}
	}


	/**
	 * Write the given element into the backend.
	 *
	 * @param value value to be written
	 * @throws Exception
	 */
	protected abstract void sendValue(Iterable<IN> value) throws Exception;

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		IN value = element.getValue();
		//generate initial operator state
		if (out == null) {
			out = getStateBackend().createCheckpointStateOutputView(0, 0);
		}
		serializer.serialize(value, out);
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		//don't do anything, since we are a sink
	}

	/**
	 * This state is used to keep a list of all StateHandles (essentially references to past OperatorStates) that were
	 * used since the last completed checkpoint.
	 **/
	public class ExactlyOnceState implements StateHandle<Serializable> {
		protected TreeMap<Long, StateHandle<DataInputView>> pendingHandles;

		public ExactlyOnceState() {
			pendingHandles = new TreeMap<>();
		}

		@Override
		public TreeMap<Long, StateHandle<DataInputView>> getState(ClassLoader userCodeClassLoader) throws Exception {
			return pendingHandles;
		}

		@Override
		public void discardState() throws Exception {
			for (StateHandle<DataInputView> handle : pendingHandles.values()) {
				handle.discardState();
			}
			pendingHandles = new TreeMap<>();
		}

		@Override
		public long getStateSize() throws Exception {
			int stateSize = 0;
			for (StateHandle<DataInputView> handle : pendingHandles.values()) {
				stateSize += handle.getStateSize();
			}
			return stateSize;
		}
	}
}
