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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.JavaSerializer;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Abstract base class for data sources that receive elements from a message queue and
 * acknowledge them back by IDs.
 *
 * <p>The mechanism for this source assumes that messages are identified by a unique ID.
 * When messages are taken from the message queue, the message must not be dropped immediately,
 * but must be retained until acknowledged. Messages that are not acknowledged within a certain
 * time interval will be served again (to a different connection, established by the recovered source).
 *
 * <p>Note that this source can give no guarantees about message order in the case of failures,
 * because messages that were retrieved but not yet acknowledged will be returned later again, after
 * a set of messages that was not retrieved before the failure.
 *
 * <p>Internally, this source gathers the IDs of elements it emits. Per checkpoint, the IDs are stored and
 * acknowledged when the checkpoint is complete. That way, no message is acknowledged unless it is certain
 * that it has been successfully processed throughout the topology and the updates to any state caused by
 * that message are persistent.
 *
 * <p>All messages that are emitted and successfully processed by the streaming program will eventually be
 * acknowledged. In corner cases, the source may receive certain IDs multiple times, if a
 * failure occurs while acknowledging. To cope with this situation, an additional Set stores all
 * processed IDs. IDs are only removed after they have been acknowledged.
 *
 * <p>A typical way to use this base in a source function is by implementing a run() method as follows:
 * <pre>{@code
 * public void run(SourceContext<Type> ctx) throws Exception {
 *     while (running) {
 *         Message msg = queue.retrieve();
 *         synchronized (ctx.getCheckpointLock()) {
 *             if (addId(msg.getMessageId())) {
 *                 ctx.collect(msg.getMessageData());
 *             }
 *         }
 *     }
 * }
 * }</pre>
 *
 * <b>NOTE:</b> This source has a parallelism of {@code 1}.
 *
 * @param <Type> The type of the messages created by the source.
 * @param <UId> The type of unique IDs which may be used to acknowledge elements.
 */
@PublicEvolving
public abstract class MessageAcknowledgingSourceBase<Type, UId>
	extends RichSourceFunction<Type>
	implements CheckpointedFunction, CheckpointListener {

	private static final long serialVersionUID = -8689291992192955579L;

	private static final Logger LOG = LoggerFactory.getLogger(MessageAcknowledgingSourceBase.class);

	/** Serializer used to serialize the IDs for checkpoints. */
	private final TypeSerializer<UId> idSerializer;

	/** The list gathering the IDs of messages emitted during the current checkpoint. */
	private transient Set<UId> idsForCurrentCheckpoint;

	/**
	 * The list with IDs from checkpoints that were triggered, but not yet completed or notified of
	 * completion.
	 */
	protected transient ArrayDeque<Tuple2<Long, Set<UId>>> pendingCheckpoints;

	/**
	 * Set which contain all processed ids. Ids are acknowledged after checkpoints. When restoring
	 * a checkpoint, ids may be processed again. This happens when the checkpoint completed but the
	 * ids for a checkpoint haven't been acknowledged yet.
	 */
	private transient Set<UId> idsProcessedButNotAcknowledged;

	private transient ListState<SerializedCheckpointData[]> checkpointedState;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new MessageAcknowledgingSourceBase for IDs of the given type.
	 *
	 * @param idClass The class of the message ID type, used to create a serializer for the message IDs.
	 */
	protected MessageAcknowledgingSourceBase(Class<UId> idClass) {
		this(TypeExtractor.getForClass(idClass));
	}

	/**
	 * Creates a new MessageAcknowledgingSourceBase for IDs of the given type.
	 *
	 * @param idTypeInfo The type information of the message ID type, used to create a serializer for the message IDs.
	 */
	protected MessageAcknowledgingSourceBase(TypeInformation<UId> idTypeInfo) {
		this.idSerializer = idTypeInfo.createSerializer(new ExecutionConfig());
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		Preconditions.checkState(this.checkpointedState == null,
			"The " + getClass().getSimpleName() + " has already been initialized.");

		// We are using JavaSerializer from the flink-runtime module here. This is very naughty and
		// we shouldn't be doing it because ideally nothing in the API modules/connector depends
		// directly on flink-runtime. We are doing it here because we need to maintain backwards
		// compatibility with old state and because we will have to rework/remove this code soon.
		this.checkpointedState = context
			.getOperatorStateStore()
			.getListState(new ListStateDescriptor<>("message-acknowledging-source-state", new JavaSerializer<>()));

		this.idsForCurrentCheckpoint = new HashSet<>(64);
		this.pendingCheckpoints = new ArrayDeque<>();
		this.idsProcessedButNotAcknowledged = new HashSet<>();

		if (context.isRestored()) {
			LOG.info("Restoring state for the {}.", getClass().getSimpleName());

			List<SerializedCheckpointData[]> retrievedStates = new ArrayList<>();
			for (SerializedCheckpointData[] entry : this.checkpointedState.get()) {
				retrievedStates.add(entry);
			}

			// given that the parallelism of the function is 1, we can only have at most 1 state
			Preconditions.checkArgument(retrievedStates.size() == 1,
				getClass().getSimpleName() + " retrieved invalid state.");

			pendingCheckpoints = SerializedCheckpointData.toDeque(retrievedStates.get(0), idSerializer);
			// build a set which contains all processed ids. It may be used to check if we have
			// already processed an incoming message.
			for (Tuple2<Long, Set<UId>> checkpoint : pendingCheckpoints) {
				idsProcessedButNotAcknowledged.addAll(checkpoint.f1);
			}
		} else {
			LOG.info("No state to restore for the {}.", getClass().getSimpleName());
		}
	}

	@Override
	public void close() throws Exception {
		idsForCurrentCheckpoint.clear();
		pendingCheckpoints.clear();
	}


	// ------------------------------------------------------------------------
	//  ID Checkpointing
	// ------------------------------------------------------------------------

	/**
	 * This method must be implemented to acknowledge the given set of IDs back to the message queue.
	 *
	 * @param uIds The list od IDs to acknowledge.
	 */
	protected abstract void acknowledgeIDs(long checkpointId, Set<UId> uIds);

	/**
	 * Adds an ID to be stored with the current checkpoint. In order to achieve exactly-once guarantees, implementing
	 * classes should only emit records with IDs for which this method return true.
	 * @param uid The ID to add.
	 * @return True if the id has not been processed previously.
	 */
	protected boolean addId(UId uid) {
		idsForCurrentCheckpoint.add(uid);
		return idsProcessedButNotAcknowledged.add(uid);
	}


	// ------------------------------------------------------------------------
	//  Checkpointing the data
	// ------------------------------------------------------------------------

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		Preconditions.checkState(this.checkpointedState != null,
			"The " + getClass().getSimpleName() + " has not been properly initialized.");

		if (LOG.isDebugEnabled()) {
			LOG.debug("Checkpointing: Messages: {}, checkpoint id: {}, timestamp: {}",
				idsForCurrentCheckpoint, context.getCheckpointId(), context.getCheckpointTimestamp());
		}

		pendingCheckpoints.addLast(new Tuple2<>(context.getCheckpointId(), idsForCurrentCheckpoint));
		idsForCurrentCheckpoint = new HashSet<>(64);

		this.checkpointedState.clear();
		this.checkpointedState.add(SerializedCheckpointData.fromDeque(pendingCheckpoints, idSerializer));
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		LOG.debug("Committing Messages externally for checkpoint {}", checkpointId);

		for (Iterator<Tuple2<Long, Set<UId>>> iter = pendingCheckpoints.iterator(); iter.hasNext();) {
			Tuple2<Long, Set<UId>> checkpoint = iter.next();
			long id = checkpoint.f0;

			if (id <= checkpointId) {
				LOG.trace("Committing Messages with following IDs {}", checkpoint.f1);
				acknowledgeIDs(checkpointId, checkpoint.f1);
				// remove deduplication data
				idsProcessedButNotAcknowledged.removeAll(checkpoint.f1);
				// remove checkpoint data
				iter.remove();
			}
			else {
				break;
			}
		}
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) {
	}
}
