/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.io.disk.InputViewIterator;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.util.ReusingMutableToRegularIteratorWrapper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamCheckpointedOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Generic Sink that emits its input elements into an arbitrary backend. This sink is integrated with Flink's checkpointing
 * mechanism and can provide exactly-once guarantees; depending on the storage backend and sink/committer implementation.
 * <p/>
 * Incoming records are stored within a {@link org.apache.flink.runtime.state.AbstractStateBackend}, and only committed if a
 * checkpoint is completed.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public abstract class GenericWriteAheadSink<IN> extends AbstractStreamOperator<IN>
		implements OneInputStreamOperator<IN, IN>, StreamCheckpointedOperator {

	private static final long serialVersionUID = 1L;

	protected static final Logger LOG = LoggerFactory.getLogger(GenericWriteAheadSink.class);

	private final String id;
	private final CheckpointCommitter committer;
	protected final TypeSerializer<IN> serializer;

	private transient CheckpointStreamFactory.CheckpointStateOutputStream out;
	private transient CheckpointStreamFactory checkpointStreamFactory;

	private final Set<PendingCheckpoint> pendingCheckpoints = new TreeSet<>();

	public GenericWriteAheadSink(CheckpointCommitter committer,	TypeSerializer<IN> serializer, String jobID) throws Exception {
		this.committer = Preconditions.checkNotNull(committer);
		this.serializer = Preconditions.checkNotNull(serializer);
		this.id = UUID.randomUUID().toString();

		this.committer.setJobId(jobID);
		this.committer.createResource();
	}

	@Override
	public void open() throws Exception {
		super.open();
		committer.setOperatorId(id);
		committer.open();

		checkpointStreamFactory = getContainingTask().createCheckpointStreamFactory(this);

		cleanRestoredHandles();
	}

	public void close() throws Exception {
		committer.close();
	}

	/**
	 * Called when a checkpoint barrier arrives. It closes any open streams to the backend
	 * and marks them as pending for committing to the external, third-party storage system.
	 *
	 * @param checkpointId the id of the latest received checkpoint.
	 * @throws IOException in case something went wrong when handling the stream to the backend.
	 */
	private void saveHandleInState(final long checkpointId, final long timestamp) throws Exception {
		//only add handle if a new OperatorState was created since the last snapshot
		if (out != null) {
			int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
			StreamStateHandle handle = out.closeAndGetHandle();

			PendingCheckpoint pendingCheckpoint = new PendingCheckpoint(checkpointId, subtaskIdx, timestamp, handle);

			if (pendingCheckpoints.contains(pendingCheckpoint)) {
				//we already have a checkpoint stored for that ID that may have been partially written,
				//so we discard this "alternate version" and use the stored checkpoint
				handle.discardState();
			} else {
				pendingCheckpoints.add(pendingCheckpoint);
			}
			out = null;
		}
	}

	@Override
	public void snapshotState(FSDataOutputStream out, long checkpointId, long timestamp) throws Exception {
		saveHandleInState(checkpointId, timestamp);

		DataOutputViewStreamWrapper outStream = new DataOutputViewStreamWrapper(out);
		outStream.writeInt(pendingCheckpoints.size());
		for (PendingCheckpoint pendingCheckpoint : pendingCheckpoints) {
			pendingCheckpoint.serialize(outStream);
		}
	}

	@Override
	public void restoreState(FSDataInputStream in) throws Exception {
		final DataInputViewStreamWrapper inStream = new DataInputViewStreamWrapper(in);
		int numPendingHandles = inStream.readInt();
		for (int i = 0; i < numPendingHandles; i++) {
			pendingCheckpoints.add(PendingCheckpoint.restore(inStream, getUserCodeClassloader()));
		}
	}

	/**
	 * Called at {@link #open()} to clean-up the pending handle list.
	 * It iterates over all restored pending handles, checks which ones are already
	 * committed to the outside storage system and removes them from the list.
	 */
	private void cleanRestoredHandles() throws Exception {
		synchronized (pendingCheckpoints) {

			Iterator<PendingCheckpoint> pendingCheckpointIt = pendingCheckpoints.iterator();
			while (pendingCheckpointIt.hasNext()) {
				PendingCheckpoint pendingCheckpoint = pendingCheckpointIt.next();

				if (committer.isCheckpointCommitted(pendingCheckpoint.subtaskId, pendingCheckpoint.checkpointId)) {
					pendingCheckpoint.stateHandle.discardState();
					pendingCheckpointIt.remove();
				}
			}
		}
	}

	@Override
	public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
		super.notifyOfCompletedCheckpoint(checkpointId);

		synchronized (pendingCheckpoints) {
			Iterator<PendingCheckpoint> pendingCheckpointIt = pendingCheckpoints.iterator();
			while (pendingCheckpointIt.hasNext()) {
				PendingCheckpoint pendingCheckpoint = pendingCheckpointIt.next();
				long pastCheckpointId = pendingCheckpoint.checkpointId;
				int subtaskId = pendingCheckpoint.subtaskId;
				long timestamp = pendingCheckpoint.timestamp;
				StreamStateHandle streamHandle = pendingCheckpoint.stateHandle;

				if (pastCheckpointId <= checkpointId) {
					try {
						if (!committer.isCheckpointCommitted(subtaskId, pastCheckpointId)) {
							try (FSDataInputStream in = streamHandle.openInputStream()) {
								boolean success = sendValues(
										new ReusingMutableToRegularIteratorWrapper<>(
												new InputViewIterator<>(
														new DataInputViewStreamWrapper(
																in),
														serializer),
												serializer),
										timestamp);
								if (success) {
									// in case the checkpoint was successfully committed,
									// discard its state from the backend and mark it for removal
									// in case it failed, we retry on the next checkpoint
									committer.commitCheckpoint(subtaskId, pastCheckpointId);
									streamHandle.discardState();
									pendingCheckpointIt.remove();
								}
							}
						} else {
							streamHandle.discardState();
							pendingCheckpointIt.remove();
						}
					} catch (Exception e) {
						// we have to break here to prevent a new (later) checkpoint
						// from being committed before this one
						LOG.error("Could not commit checkpoint.", e);
						break;
					}
				}
			}
		}
	}

	/**
	 * Write the given element into the backend.
	 *
	 * @param value value to be written
	 * @return true, if the sending was successful, false otherwise
	 * @throws Exception
	 */
	protected abstract boolean sendValues(Iterable<IN> value, long timestamp) throws Exception;

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		IN value = element.getValue();
		// generate initial operator state
		if (out == null) {
			out = checkpointStreamFactory.createCheckpointStateOutputStream(0, 0);
		}
		serializer.serialize(value, new DataOutputViewStreamWrapper(out));
	}

	private static final class PendingCheckpoint implements Comparable<PendingCheckpoint>, Serializable {

		private static final long serialVersionUID = -3571036395734603443L;

		private final long checkpointId;
		private final int subtaskId;
		private final long timestamp;
		private final StreamStateHandle stateHandle;

		PendingCheckpoint(long checkpointId, int subtaskId, long timestamp, StreamStateHandle handle) {
			this.checkpointId = checkpointId;
			this.subtaskId = subtaskId;
			this.timestamp = timestamp;
			this.stateHandle = handle;
		}

		void serialize(DataOutputViewStreamWrapper outputStream) throws IOException {
			outputStream.writeLong(checkpointId);
			outputStream.writeInt(subtaskId);
			outputStream.writeLong(timestamp);
			InstantiationUtil.serializeObject(outputStream, stateHandle);
		}

		static PendingCheckpoint restore(
				DataInputViewStreamWrapper inputStream,
				ClassLoader classLoader) throws IOException, ClassNotFoundException {

			long checkpointId = inputStream.readLong();
			int subtaskId = inputStream.readInt();
			long timestamp = inputStream.readLong();
			StreamStateHandle handle = InstantiationUtil.deserializeObject(inputStream, classLoader);

			return new PendingCheckpoint(checkpointId, subtaskId, timestamp, handle);
		}

		@Override
		public int compareTo(PendingCheckpoint o) {
			int res = Long.compare(this.checkpointId, o.checkpointId);
			return res != 0 ? res : Integer.compare(this.subtaskId, o.subtaskId);
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof GenericWriteAheadSink.PendingCheckpoint)) {
				return false;
			}
			PendingCheckpoint other = (PendingCheckpoint) o;
			return this.checkpointId == other.checkpointId &&
				this.subtaskId == other.subtaskId &&
				this.timestamp == other.timestamp;
		}

		@Override
		public int hashCode() {
			int hash = 17;
			hash = 31 * hash + (int) (checkpointId ^ (checkpointId >>> 32));
			hash = 31 * hash + subtaskId;
			hash = 31 * hash + (int) (timestamp ^ (timestamp >>> 32));
			return hash;
		}
	}
}
