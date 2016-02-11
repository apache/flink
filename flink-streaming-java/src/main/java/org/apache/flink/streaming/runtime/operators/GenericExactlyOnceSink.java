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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Generic Sink that emits its input elements into an arbitrary backend. This sink is integrated with the checkpointing
 * mechanism to provide exactly once semantics.
 *
 * Incoming records are stored within a {@link org.apache.flink.runtime.state.AbstractStateBackend}, and only committed if a
 * checkpoint is completed. Should a job fail while the data is being committed, no exactly once guarantee can be made.
 * @param <IN> Type of the elements emitted by this sink
 */
public abstract class GenericExactlyOnceSink<IN> extends AbstractStreamOperator<IN> implements OneInputStreamOperator<IN, IN> {
	private AbstractStateBackend.CheckpointStateOutputView out;
	private TypeSerializer<IN> serializer;
	protected TypeInformation<IN> typeInfo;

	private ExactlyOnceState state = new ExactlyOnceState();

	/**
	 * Saves a handle in the state.
	 * @param checkpointId
	 * @throws IOException
	 */
	private void saveHandleInState(final long checkpointId) throws IOException {
		//only add handle if a new OperatorState was created since the last snapshot/notify
		if (out != null) {
			out.writeByte(0); //EOF-byte
			StateHandle<DataInputView> handle = out.closeAndGetHandle();
			if (state.pendingHandles.containsKey(checkpointId)) {
				state.pendingHandles.get(checkpointId).add(handle);
			} else {
				ArrayList<StateHandle<DataInputView>> list = new ArrayList<>();
				list.add(handle);
				state.pendingHandles.put(checkpointId, list);
			}
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
		saveHandleInState(checkpointId);

		synchronized (state.pendingHandles) {
			Set<Long> pastCheckpointIds = state.pendingHandles.keySet();
			Set<Long> checkpointsToRemove = new HashSet<>();
			for (Long pastCheckpointId : pastCheckpointIds) {
				if (pastCheckpointId <= checkpointId) {
					List<StateHandle<DataInputView>> handles = state.pendingHandles.get(pastCheckpointId);
					for (StateHandle<DataInputView> handle : handles) {
						DataInputView in = handle.getState(getUserCodeClassloader());
						while (in.readByte() == 1) {
							IN value = serializer.deserialize(in);
							sendValue(value);
						}
					}
					checkpointsToRemove.add(pastCheckpointId);
				}
			}
			for (Long toRemove : checkpointsToRemove) {
				state.pendingHandles.remove(toRemove);
			}
		}
	}

	/**
	 * Write the given element into the backend.
	 * @param value value to be written
	 * @throws Exception
     */
	protected abstract void sendValue(IN value) throws Exception;

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		IN value = element.getValue();
		if (serializer == null) {
			typeInfo = TypeExtractor.getForObject(value);
			serializer = typeInfo.createSerializer(new ExecutionConfig());
		}
		//generate initial operator state
		if (out == null) {
			out = getStateBackend().createCheckpointStateOutputView(0, 0);
		}
		out.writeByte(1);
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
		protected HashMap<Long, ArrayList<StateHandle<DataInputView>>> pendingHandles;

		public ExactlyOnceState() {
			pendingHandles = new HashMap<>();
		}

		@Override
		public HashMap<Long, ArrayList<StateHandle<DataInputView>>> getState(ClassLoader userCodeClassLoader) throws Exception {
			return pendingHandles;
		}

		@Override
		public void discardState() throws Exception {
			pendingHandles = new HashMap<>();
		}

		@Override
		public long getStateSize() throws Exception {
			return 0;
		}
	}
}
