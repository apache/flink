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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointNotifier;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.runtime.state.SerializedCheckpointData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for data sources that receive elements from a message queue and
 * acknowledge them back by IDs.
 * <p>
 * The mechanism for this source assumes that messages are identified by a unique ID.
 * When messages are taken from the message queue, the message must not be dropped immediately,
 * but must be retained until acknowledged. Messages that are not acknowledged within a certain
 * time interval will be served again (to a different connection, established by the recovered source).
 * <p>
 * Note that this source can give no guarantees about message order in teh case of failures,
 * because messages that were retrieved but not yet acknowledged will be returned later again, after
 * a set of messages that was not retrieved before the failure.
 * <p>
 * Internally, this source gathers the IDs of elements it emits. Per checkpoint, the IDs are stored and
 * acknowledged when the checkpoint is complete. That way, no message is acknowledged unless it is certain
 * that it has been successfully processed throughout the topology and the updates to any state caused by
 * that message are persistent.
 * <p>
 * All messages that are emitted and successfully processed by the streaming program will eventually be
 * acknowledged. In corner cases, the source may acknowledge certain IDs multiple times, if a
 * failure occurs while acknowledging.
 * <p>
 * A typical way to use this base in a source function is by implementing a run() method as follows:
 * <pre>{@code
 * public void run(SourceContext<Type> ctx) throws Exception {
 *     while (running) {
 *         Message msg = queue.retrieve();
 *         synchronized (ctx.getCheckpointLock()) {
 *             ctx.collect(msg.getMessageData());
 *             addId(msg.getMessageId());
 *         }
 *     }
 * }
 * }</pre>
 * 
 * @param <Type> The type of the messages created by the source.
 * @param <Id> The type of the IDs that are used for acknowledging elements.
 */
public abstract class MessageAcknowledingSourceBase<Type, Id> extends RichSourceFunction<Type> 
	implements Checkpointed<SerializedCheckpointData[]>, CheckpointNotifier {
	
	private static final long serialVersionUID = -8689291992192955579L;
	
	private static final Logger LOG = LoggerFactory.getLogger(MessageAcknowledingSourceBase.class);
	
	/** Serializer used to serialize the IDs for checkpoints */
	private final TypeSerializer<Id> idSerializer;
	
	/** The list gathering the IDs of messages emitted during the current checkpoint */
	private transient List<Id> idsForCurrentCheckpoint;

	/** The list with IDs from checkpoints that were triggered, but not yet completed or notified of completion */
	private transient ArrayDeque<Tuple2<Long, List<Id>>> pendingCheckpoints;
	
	// ------------------------------------------------------------------------

	/**
	 * Creates a new MessageAcknowledingSourceBase for IDs of teh given type.
	 * 
	 * @param idClass The class of the message ID type, used to create a serializer for the message IDs.
	 */
	protected MessageAcknowledingSourceBase(Class<Id> idClass) {
		this(TypeExtractor.getForClass(idClass));
	}

	/**
	 * Creates a new MessageAcknowledingSourceBase for IDs of teh given type.
	 * 
	 * @param idTypeInfo The type information of the message ID type, used to create a serializer for the message IDs.
	 */
	protected MessageAcknowledingSourceBase(TypeInformation<Id> idTypeInfo) {
		this.idSerializer = idTypeInfo.createSerializer(new ExecutionConfig());
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		idsForCurrentCheckpoint = new ArrayList<>(64);
		pendingCheckpoints = new ArrayDeque<>();
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
	 * @param ids The list od IDs to acknowledge.
	 */
	protected abstract void acknowledgeIDs(List<Id> ids);

	/**
	 * Adds an ID to be stored with the current checkpoint.
	 * @param id The ID to add.
	 */
	protected void addId(SourceContext<Type> ctx, Id id) {
		idsForCurrentCheckpoint.add(id);
	}

	// ------------------------------------------------------------------------
	//  Checkpointing the data
	// ------------------------------------------------------------------------
	
	@Override
	public SerializedCheckpointData[] snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Snapshotting state. Messages: {}, checkpoint id: {}, timestamp: {}",
					idsForCurrentCheckpoint, checkpointId, checkpointTimestamp);
		}
		
		pendingCheckpoints.addLast(new Tuple2<Long, List<Id>>(checkpointId, idsForCurrentCheckpoint));
		idsForCurrentCheckpoint = new ArrayList<>(64);
		
		return SerializedCheckpointData.fromDeque(pendingCheckpoints, idSerializer);
	}

	@Override
	public void restoreState(SerializedCheckpointData[] state) throws Exception {
		pendingCheckpoints = SerializedCheckpointData.toDeque(state, idSerializer);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {	
		// only one commit operation must be in progress
		if (LOG.isDebugEnabled()) {
			LOG.debug("Committing Messages externally for checkpoint {}", checkpointId);
		}
		
		for (Iterator<Tuple2<Long, List<Id>>> iter = pendingCheckpoints.iterator(); iter.hasNext();) {
			Tuple2<Long, List<Id>> checkpoint = iter.next();
			long id = checkpoint.f0;
			
			if (id <= checkpointId) {
				if (LOG.isTraceEnabled()) {
					LOG.trace("Committing Messages with following IDs {}", checkpoint.f1);
				}
				acknowledgeIDs(checkpoint.f1);
				iter.remove();
			}
			else {
				break;
			}
		}
	}
}
