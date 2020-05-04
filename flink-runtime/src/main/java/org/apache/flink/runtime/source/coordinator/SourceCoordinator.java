/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * The default implementation of the {@link OperatorCoordinator} for the {@link Source}.
 *
 * <p>The <code>SourceCoordinator</code> provides an event loop style thread model to interact with
 * the Flink runtime. The coordinator ensures that all the state manipulations are made by its event loop
 * thread. It also helps keep track of the necessary split assignments history per subtask to simplify the
 * {@link SplitEnumerator} implementation.
 *
 * <p>The coordinator maintains a {@link org.apache.flink.api.connector.source.SplitEnumeratorContext
 * SplitEnumeratorContxt} and shares it with the enumerator. When the coordinator receives an action
 * request from the Flink runtime, it sets up the context, and calls corresponding method of the
 * SplitEnumerator to take actions.
 */
@Internal
public class SourceCoordinator<SplitT extends SourceSplit, EnumChkT> implements OperatorCoordinator {
	private static final Logger LOG = LoggerFactory.getLogger(OperatorCoordinator.class);
	/** A single-thread executor to handle all the changes to the coordinator. */
	private final ExecutorService coordinatorExecutor;
	/** The Source that is associated with this SourceCoordinator. */
	private final Source<?, SplitT, EnumChkT> source;
	/** The serializer that handles the serde of the SplitEnumerator checkpoints. */
	private final SimpleVersionedSerializer<EnumChkT> enumCheckpointSerializer;
	/** The serializer for the SourceSplit of the associated Source. */
	private final SimpleVersionedSerializer<SplitT> splitSerializer;
	/** The context containing the states of the coordinator. */
	private final SourceCoordinatorContext<SplitT> context;
	/** The split enumerator created from the associated Source. */
	private SplitEnumerator<SplitT, EnumChkT> enumerator;
	/** A flag marking whether the coordinator has started. */
	private boolean started;

	public SourceCoordinator(ExecutorService coordinatorExecutor,
							 Source<?, SplitT, EnumChkT> source,
							 SourceCoordinatorContext<SplitT> context) {
		this.coordinatorExecutor = coordinatorExecutor;
		this.source = source;
		this.enumCheckpointSerializer = source.getEnumeratorCheckpointSerializer();
		this.splitSerializer = source.getSplitSerializer();
		this.context = context;
		this.enumerator = source.createEnumerator(context);
		this.started = false;
	}

	@Override
	public void start() throws Exception {
		LOG.info("Starting split enumerator.");
		enumerator.start();
		started = true;
	}

	@Override
	public void close() throws Exception {
		ensureStarted();
		LOG.info("Closing SourceCoordinator.");
		enumerator.close();
		coordinatorExecutor.shutdown();
		LOG.info("Source coordinator closed.");
	}

	@Override
	@SuppressWarnings("unchecked")
	public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
		ensureStarted();
		coordinatorExecutor.execute(() -> {
			LOG.debug("Handling event from operator: {}", event);
			if (event instanceof SourceEventWrapper) {
				enumerator.handleSourceEvent(subtask, ((SourceEventWrapper) event).getSourceEvent());
			} else if (event instanceof ReaderRegistrationEvent) {
				handleReaderRegistrationEvent((ReaderRegistrationEvent) event);
			}
		});
	}

	@Override
	public void subtaskFailed(int subtaskId) {
		ensureStarted();
		coordinatorExecutor.execute(() -> {
			LOG.info("Handling subtask {} failure.", subtaskId);
			List<SplitT> splitsToAddBack = context.getAndRemoveUncheckpointedAssignment(subtaskId);
			context.unregisterSourceReader(subtaskId);
			LOG.debug("Adding {} back to the split enumerator.", splitsToAddBack);
			enumerator.addSplitsBack(splitsToAddBack, subtaskId);
		});
	}

	@Override
	public CompletableFuture<byte[]> checkpointCoordinator(long checkpointId) throws Exception {
		ensureStarted();
		return CompletableFuture.supplyAsync(() -> {
			try {
				LOG.debug("Taking a state snapshot for checkpoint {}", checkpointId);
				return toBytes(checkpointId);
			} catch (Exception e) {
				throw new FlinkRuntimeException("Failed to checkpoint coordinator due to ", e);
			}
		}, coordinatorExecutor);
	}

	@Override
	public void checkpointComplete(long checkpointId) {
		ensureStarted();
		coordinatorExecutor.execute(() -> {
			LOG.info("Marking checkpoint {} as completed.", checkpointId);
			context.onCheckpointComplete(checkpointId);
		});
	}

	@Override
	public void resetToCheckpoint(byte[] checkpointData) throws Exception {
		if (started) {
			throw new IllegalStateException(
					"The source coordinator has started. The source coordinator state can " +
					"only be reset to a checkpoint before it starts.");
		}
		LOG.info("Resetting SourceCoordinator from checkpoint.");
		fromBytes(checkpointData);
	}

	// ---------------------------------------------------
	@VisibleForTesting
	SplitEnumerator<SplitT, EnumChkT> getEnumerator() {
		return enumerator;
	}

	@VisibleForTesting
	SourceCoordinatorContext<SplitT> getContext() {
		return context;
	}

	// --------------------- Serde -----------------------

	/**
	 * Serialize the coordinator state. The current implementation may not be super efficient,
	 * but it should not matter that much because most of the state should be rather small.
	 * Large states themselves may already be a problem regardless of how the serialization
	 * is implemented.
	 *
	 * @return A byte array containing the serialized state of the source coordinator.
	 * @throws Exception When something goes wrong in serialization.
	 */
	private byte[] toBytes(long checkpointId) throws Exception {
		EnumChkT enumCkpt = enumerator.snapshotState();

		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutput out = new ObjectOutputStream(baos)) {
			out.writeInt(enumCheckpointSerializer.getVersion());
			out.writeObject(enumCheckpointSerializer.serialize(enumCkpt));
			context.snapshotState(checkpointId, splitSerializer, out);
			out.flush();
			return baos.toByteArray();
		}
	}

	/**
	 * Restore the state of this source coordinator from the state bytes.
	 *
	 * @param bytes The checkpoint bytes that was returned from {@link #toBytes(long)}
	 * @throws Exception When the deserialization failed.
	 */
	private void fromBytes(byte[] bytes) throws Exception {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			ObjectInput in = new ObjectInputStream(bais)) {
			int enumSerializerVersion = in.readInt();
			byte[] serializedEnumChkpt = (byte[]) in.readObject();
			EnumChkT enumChkpt = enumCheckpointSerializer.deserialize(enumSerializerVersion, serializedEnumChkpt);
			context.restoreState(splitSerializer, in);
			enumerator = source.restoreEnumerator(context, enumChkpt);
		}
	}

	// --------------------- private methods -------------

	private void handleReaderRegistrationEvent(ReaderRegistrationEvent event) {
		context.registerSourceReader(new ReaderInfo(event.subtaskId(), event.location()));
		enumerator.addReader(event.subtaskId());
	}

	private void ensureStarted() {
		if (!started) {
			throw new IllegalStateException("The coordinator has not started yet.");
		}
	}
}
