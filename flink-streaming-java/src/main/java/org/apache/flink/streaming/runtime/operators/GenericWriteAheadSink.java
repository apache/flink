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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Generic Sink that emits its input elements into an arbitrary backend. This sink is integrated with
 * Flink's checkpointing mechanism and can provide exactly-once guarantees; depending on the storage
 * backend and sink/committer implementation.
 * <p/>
 * Incoming records are stored within a {@link org.apache.flink.runtime.state.AbstractStateBackend}, and
 * only committed if a checkpoint is completed.
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

	private final TreeMap<PendingCheckpointId, PendingHandle> pendingHandles = new TreeMap<>();

	public GenericWriteAheadSink(
			CheckpointCommitter committer,
			TypeSerializer<IN> serializer,
			String jobID) throws Exception {

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

		checkpointStreamFactory = getContainingTask()
			.createCheckpointStreamFactory(this);

		cleanRestoredHandles();
	}

	public void close() throws Exception {
		committer.close();
	}

	/**
	 * Called when a checkpoint barrier arrives.
	 * Closes any open streams to the backend and marks them as pending for
	 * committing to the final output system, e.g. Cassandra.
	 *
	 * @param checkpointId the id of the latest received checkpoint.
	 * @throws IOException in case something went wrong when handling the stream to the backend.
	 */
	private void saveHandleInState(final long checkpointId, final long timestamp) throws Exception {
		Preconditions.checkNotNull(this.pendingHandles, "The operator has not been properly initialized.");

		//only add handle if a new OperatorState was created since the last snapshot
		if (out != null) {
			StreamStateHandle handle = out.closeAndGetHandle();

			PendingCheckpointId pendingCheckpoint = new PendingCheckpointId(
				checkpointId, getRuntimeContext().getIndexOfThisSubtask());

			if (pendingHandles.containsKey(pendingCheckpoint)) {
				//we already have a checkpoint stored for that ID that may have been partially written,
				//so we discard this "alternate version" and use the stored checkpoint
				handle.discardState();
			} else {
				this.pendingHandles.put(pendingCheckpoint, new PendingHandle(timestamp, handle));
			}
			out = null;
		}
	}

	@Override
	public void snapshotState(FSDataOutputStream out, long checkpointId, long timestamp) throws Exception {
		saveHandleInState(checkpointId, timestamp);

		DataOutputViewStreamWrapper outStream = new DataOutputViewStreamWrapper(out);
		outStream.writeInt(pendingHandles.size());
		for (Map.Entry<PendingCheckpointId, PendingHandle> pendingCheckpoint : pendingHandles.entrySet()) {
			pendingCheckpoint.getKey().serialize(outStream);
			pendingCheckpoint.getValue().serialize(outStream);
		}
	}

	@Override
	public void restoreState(FSDataInputStream in) throws Exception {
		final DataInputViewStreamWrapper inStream = new DataInputViewStreamWrapper(in);
		int noOfPendingHandles = inStream.readInt();
		for (int i = 0; i < noOfPendingHandles; i++) {
			PendingCheckpointId id = PendingCheckpointId.restore(inStream);
			PendingHandle handle = PendingHandle.restore(inStream, getUserCodeClassloader());
			this.pendingHandles.put(id, handle);
		}
	}

	/**
	 * Called at {@link #open()} to clean-up the pending handle list.
	 * It iterates over all restored pending handles, checks which ones are already
	 * committed to the outside storage system and removes them from the list.
	 */
	private void cleanRestoredHandles() throws Exception {
		synchronized (pendingHandles) {

			Set<PendingCheckpointId> checkpointIdsToRemove = new HashSet<>();

			// for each of the pending handles...
			for (Map.Entry<PendingCheckpointId, PendingHandle> restoredHandle : pendingHandles.entrySet()) {
				PendingCheckpointId id = restoredHandle.getKey();
				PendingHandle handle = restoredHandle.getValue();

				//...check if the temporary buffer is already committed and if yes,
				// add it in the list of checkpoints to be permanently removed...
				if (committer.isCheckpointCommitted(id.subtaskId, id.checkpointId)) {
					handle.stateHandle.discardState();
					checkpointIdsToRemove.add(id);
				}
			}

			// ...finally cleanup the map of pending handles per checkpoint id.
			for (PendingCheckpointId checkpointId: checkpointIdsToRemove) {
				pendingHandles.remove(checkpointId);
			}
		}
	}

	@Override
	public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
		super.notifyOfCompletedCheckpoint(checkpointId);

		synchronized (pendingHandles) {
			Set<PendingCheckpointId> checkpointsToRemove = new HashSet<>();

			for (Map.Entry<PendingCheckpointId, PendingHandle> pendingHandle : pendingHandles.entrySet()) {

				PendingCheckpointId pendingId = pendingHandle.getKey();
				long pastCheckpointId = pendingId.checkpointId;
				int subtaskId = pendingId.subtaskId;

				if (pastCheckpointId <= checkpointId) {

					PendingHandle handle = pendingHandle.getValue();
					long timestamp = handle.timestamp;
					StreamStateHandle streamHandle = handle.stateHandle;

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
									// in case the checkpoint was successfully committed, discard its state from the
									// backend and mark it for removal
									// in case it failed, we retry on the next checkpoint

									committer.commitCheckpoint(subtaskId, pastCheckpointId);
									streamHandle.discardState();
									checkpointsToRemove.add(pendingId);
								}
							}
						} else {
							streamHandle.discardState();
							checkpointsToRemove.add(pendingId);
						}
					} catch (Exception e) {
						// we have to break here to prevent a new (later) checkpoint
						// from being committed before this one
						LOG.error("Could not commit checkpoint.", e);
						break;
					}
				}
			}

			// ...finally remove the checkpoints that are marked as committed
			for (PendingCheckpointId toRemove : checkpointsToRemove) {
				pendingHandles.remove(toRemove);
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

	private static final class PendingCheckpointId implements Comparable<PendingCheckpointId>, Serializable {

		private static final long serialVersionUID = -3571036395734603443L;

		private final long checkpointId;
		private final int subtaskId;

		PendingCheckpointId(long checkpointId, int subtaskId) {
			this.checkpointId = checkpointId;
			this.subtaskId = subtaskId;
		}

		void serialize(DataOutputViewStreamWrapper outputStream) throws IOException {
			outputStream.writeLong(this.checkpointId);
			outputStream.writeInt(this.subtaskId);
		}

		static PendingCheckpointId restore(DataInputViewStreamWrapper inputStream) throws IOException, ClassNotFoundException {
			long checkpointId = inputStream.readLong();
			int subtaskId = inputStream.readInt();
			return new PendingCheckpointId(checkpointId, subtaskId);
		}

		@Override
		public int compareTo(PendingCheckpointId o) {
			if (o == null) {
				return -1;
			}

			long res = this.checkpointId - o.checkpointId;
			if (res == 0) {
				return this.subtaskId - o.subtaskId;
			}

			// we cannot just cast it to int as it may overflow
			return res < 0 ? -1 : 1;
		}

		@Override
		public boolean equals(Object o) {
			if (o == null || !(o instanceof PendingCheckpointId)) {
				return false;
			}
			PendingCheckpointId other = (PendingCheckpointId) o;
			return this.checkpointId == other.checkpointId &&
				this.subtaskId == other.subtaskId;
		}

		@Override
		public int hashCode() {
			return 37 * (int)(checkpointId ^ (checkpointId >>> 32)) + subtaskId;
		}
	}

	private static final class PendingHandle implements Serializable {

		private static final long serialVersionUID = -3571063495734603443L;

		private final long timestamp;
		private final StreamStateHandle stateHandle;

		PendingHandle(long timestamp, StreamStateHandle handle) {
			this.timestamp = timestamp;
			this.stateHandle = handle;
		}

		void serialize(DataOutputViewStreamWrapper outputStream) throws IOException {
			outputStream.writeLong(timestamp);
			InstantiationUtil.serializeObject(outputStream, stateHandle);
		}

		static PendingHandle restore(
			DataInputViewStreamWrapper inputStream, ClassLoader classLoader) throws IOException, ClassNotFoundException {

			long timestamp = inputStream.readLong();
			StreamStateHandle handle = InstantiationUtil.deserializeObject(inputStream, classLoader);
			return new PendingHandle(timestamp, handle);
		}
	}
}
