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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionSnapshotContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Abstract base class for data sources that receive elements from a message queue and
 * acknowledge them back by IDs. In contrast to {@link MessageAcknowledgingSourceBase}, this source
 * handles two types of ids:
 *
 * <ol>
 *   <li>Session ids
 *   <li>Unique message ids
 * </ol>
 *
 * <p>Session ids are used to acknowledge messages in a session. When a checkpoint is restored,
 * unacknowledged messages are redelivered. Duplicates are detected using the unique message ids
 * which are checkpointed.
 *
 * @param <Type> The type of the messages created by the source.
 * @param <UId> The type of the unique IDs which are consistent across sessions.
 * @param <SessionId> The type of the IDs that are used for acknowledging elements
 *                    (ids valid during session).
 */
@PublicEvolving
public abstract class MultipleIdsMessageAcknowledgingSourceBase<Type, UId, SessionId>
		extends MessageAcknowledgingSourceBase<Type, UId> {

	private static final long serialVersionUID = 42L;

	private static final Logger LOG =
		LoggerFactory.getLogger(MultipleIdsMessageAcknowledgingSourceBase.class);

	/* Session ids per pending snapshot */
	protected transient Deque<Tuple2<Long, List<SessionId>>> sessionIdsPerSnapshot;

	/* Current session ids for this snapshot */
	protected transient List<SessionId> sessionIds;

	/**
	 * Creates a new MessageAcknowledgingSourceBase for IDs of the given type.
	 *
	 * @param idClass The class of the message ID type, used to create a serializer for the message IDs.
	 */
	protected MultipleIdsMessageAcknowledgingSourceBase(Class<UId> idClass) {
		super(idClass);
	}

	/**
	 * Creates a new MessageAcknowledgingSourceBase for IDs of the given type.
	 *
	 * @param idTypeInfo The type information of the message ID type, used to create a serializer for the message IDs.
	 */
	protected MultipleIdsMessageAcknowledgingSourceBase(TypeInformation<UId> idTypeInfo) {
		super(idTypeInfo);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		sessionIds = new ArrayList<>(64);
		sessionIdsPerSnapshot = new ArrayDeque<>();
	}

	@Override
	public void close() throws Exception {
		super.close();
		sessionIds.clear();
		sessionIdsPerSnapshot.clear();
	}

	// ------------------------------------------------------------------------
	//  ID Checkpointing
	// ------------------------------------------------------------------------

	/**
	 * Acknowledges the session ids.
	 * @param checkpointId The id of the current checkout to acknowledge ids for.
	 * @param uniqueIds The checkpointed unique ids which are ignored here. They only serve as a
	 *                  means of de-duplicating messages when the acknowledgment after a checkpoint
	 *                  fails.
	 */
	@Override
	protected final void acknowledgeIDs(long checkpointId, Set<UId> uniqueIds) {
		LOG.debug("Acknowledging ids for checkpoint {}", checkpointId);
		Iterator<Tuple2<Long, List<SessionId>>> iterator = sessionIdsPerSnapshot.iterator();
		while (iterator.hasNext()) {
			final Tuple2<Long, List<SessionId>> next = iterator.next();
			long id = next.f0;
			if (id <= checkpointId) {
				acknowledgeSessionIDs(next.f1);
				// remove ids for this session
				iterator.remove();
			}
		}
	}

	/**
	 * Acknowledges the session ids.
	 * @param sessionIds The message ids for this session.
	 */
	protected abstract void acknowledgeSessionIDs(List<SessionId> sessionIds);

	// ------------------------------------------------------------------------
	//  Checkpointing the data
	// ------------------------------------------------------------------------

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		sessionIdsPerSnapshot.add(new Tuple2<>(context.getCheckpointId(), sessionIds));
		sessionIds = new ArrayList<>(64);
		super.snapshotState(context);
	}
}
