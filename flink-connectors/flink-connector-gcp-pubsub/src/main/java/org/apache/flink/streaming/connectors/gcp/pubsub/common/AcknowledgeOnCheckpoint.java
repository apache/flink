/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.gcp.pubsub.common;

import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.Collectors.toList;

/**
 * Helper class for SourceFunctions to acknowledge messages to external systems after a successful checkpoint.
 *
 * <p>The mechanism for this source assumes that messages are identified by a unique ID.
 * When messages are taken from the message queue, the message must not be dropped immediately from the external system,
 * but must be retained until acknowledged. Messages that are not acknowledged within a certain
 * time interval will be served again (to a different connection, established by the recovered source).
 *
 * <p>Note that this source can give no guarantees about message order in the case of failures,
 * because messages that were retrieved but not yet acknowledged will be returned later again, after
 * a set of messages that was not retrieved before the failure.
 *
 * <p>Internally, this class gathers the IDs of elements it emits. Per checkpoint, the IDs are stored and
 * acknowledged when the checkpoint is complete. That way, no message is acknowledged unless it is certain
 * that it has been successfully processed throughout the topology and the updates to any state caused by
 * that message are persistent.
 *
 * @param <ACKID> Type of Ids to acknowledge
 */
public class AcknowledgeOnCheckpoint<ACKID extends Serializable> implements CheckpointListener, ListCheckpointed<AcknowledgeIdsForCheckpoint<ACKID>> {
	private final Acknowledger<ACKID> acknowledger;
	private List<AcknowledgeIdsForCheckpoint<ACKID>> acknowledgeIdsPerCheckpoint;
	private List<ACKID> acknowledgeIdsForPendingCheckpoint;
	private AtomicInteger outstandingAcknowledgements;

	public AcknowledgeOnCheckpoint(Acknowledger<ACKID> acknowledger) {
		this.acknowledger = acknowledger;
		this.acknowledgeIdsPerCheckpoint = new ArrayList<>();
		this.acknowledgeIdsForPendingCheckpoint = new ArrayList<>();
		this.outstandingAcknowledgements = new AtomicInteger(0);
	}

	public void addAcknowledgeId(ACKID id) {
		acknowledgeIdsForPendingCheckpoint.add(id);
		outstandingAcknowledgements.incrementAndGet();
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		//get all acknowledgeIds of this and earlier checkpoints
		List<ACKID> idsToAcknowledge = acknowledgeIdsPerCheckpoint
			.stream()
			.filter(acknowledgeIdsForCheckpoint -> acknowledgeIdsForCheckpoint.getCheckpointId() <= checkpointId)
			.flatMap(acknowledgeIdsForCheckpoint -> acknowledgeIdsForCheckpoint.getAcknowledgeIds().stream())
			.collect(toList());

		acknowledger.acknowledge(idsToAcknowledge);

		//only keep acknowledgeIds of newer checkpointIds
		acknowledgeIdsPerCheckpoint = acknowledgeIdsPerCheckpoint.stream()
			.filter(acknowledgeIdsForCheckpoint -> acknowledgeIdsForCheckpoint.getCheckpointId() > checkpointId)
			.collect(toList());
		outstandingAcknowledgements = new AtomicInteger(numberOfAcknowledgementIds(acknowledgeIdsPerCheckpoint));
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) {
	}

	@Override
	public List<AcknowledgeIdsForCheckpoint<ACKID>> snapshotState(long checkpointId, long timestamp) {
		acknowledgeIdsPerCheckpoint.add(new AcknowledgeIdsForCheckpoint<>(checkpointId, acknowledgeIdsForPendingCheckpoint));
		acknowledgeIdsForPendingCheckpoint = new ArrayList<>();

		return acknowledgeIdsPerCheckpoint;
	}

	@Override
	public void restoreState(List<AcknowledgeIdsForCheckpoint<ACKID>> state) {
		outstandingAcknowledgements = new AtomicInteger(numberOfAcknowledgementIds(state));
		acknowledgeIdsPerCheckpoint = state;
	}

	private int numberOfAcknowledgementIds(List<AcknowledgeIdsForCheckpoint<ACKID>> acknowledgeIdsForCheckpoints) {
		return acknowledgeIdsForCheckpoints
			.stream()
			.map(AcknowledgeIdsForCheckpoint::getAcknowledgeIds)
			.mapToInt(List::size)
			.sum();
	}

	public int numberOfOutstandingAcknowledgements() {
		return outstandingAcknowledgements.get();
	}
}
