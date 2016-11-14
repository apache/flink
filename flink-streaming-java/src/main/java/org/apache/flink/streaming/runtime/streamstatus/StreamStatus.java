/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.streamstatus;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.runtime.operators.TimestampsAndPeriodicWatermarksOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

/**
 * A Stream Status element informs operators whether or not they should continue to expect records and watermarks
 * from the input stream that sent them. There are 2 kinds of status, namely {@link StreamStatus#IDLE} and
 * {@link StreamStatus#ACTIVE}. Stream Status elements are generated at the sources, and may be propagated through
 * the operators of the topology using {@link org.apache.flink.streaming.api.operators.Output#emitStreamStatus(StreamStatus)}.
 * They directly infer the current status of the emitting source or operator; a source or operator emits a
 * {@link StreamStatus#IDLE} if it will temporarily halt to emit any records or watermarks (i.e. is idle), and emits a
 * {@link StreamStatus#ACTIVE} once it resumes to do so (i.e. is active). The cases that sources and downstream operators
 * are considered either idle or active is explained below:
 *
 * <ul>
 *     <li>Sources: A source is considered to be idle if it will not emit records for an indefinite amount of time. This
 *         is the case, for example, for Flink's Kafka Consumer, where sources might initially have no assigned partitions
 *         to read from, or no records can be read from the assigned partitions. Once the source detects that it will
 *         resume emitting data, it is considered to be active. Sources are responsible for ensuring that no records (and
 *         possibly watermarks, in the case of Flink's Kafka Consumer which can generate watermarks directly within the
 *         source) will be emitted in between a consecutive {@link StreamStatus#IDLE} and {@link StreamStatus#ACTIVE}.
 *         This guarantee should be enforced on sources through
 *         {@link org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext} implementations.</li>
 *
 *     <li>Downstream operators: a downstream operator is considered to be idle if all of its input streams are idle,
 *         i.e. the last received Stream Status element from all input streams is a {@link StreamStatus#IDLE}. As long
 *         as one of its input streams is active, i.e. the last received Stream Status element from the input stream is
 *         {@link StreamStatus#ACTIVE}, the operator is active. Operators are responsible for propagating their status
 *         further downstream once they toggle between being idle and active.</li>
 * </ul>
 *
 * <p>
 * Stream Status elements received at downstream operators also affect and controls how they process and advance their
 * watermarks. The below describes the effects (the logic is implemented as a {@link StatusWatermarkValve} which
 * downstream operators should use for such purposes):
 *
 * <ul>
 *     <li>Since sources guarantee that no records will be emitted between a {@link StreamStatus#IDLE} and
 *         {@link StreamStatus#ACTIVE}, downstream operators can always safely process and propagate records when they
 *         receive them, without the need to check whether or not the operator is currently idle or active. However, for
 *         watermarks, since there may be {@link TimestampsAndPeriodicWatermarksOperator}s that might produce watermarks
 *         anywhere in the middle of topologies, regardless of whether there are input data at the operator, all
 *         downstream operators need to check whether or not they are actually active before processing a received
 *         watermark.</li>
 *
 *     <li>For downstream operators with multiple input streams (ex. head operators of a
 *         {@link org.apache.flink.streaming.runtime.tasks.OneInputStreamTask} or
 *         {@link org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask}, or any
 *         {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator}), the watermarks of input streams
 *         that are temporarily idle, or has resumed to be active but its watermark is behind the overall min watermark
 *         of the operator, should not be accounted for when deciding whether or not to advance the operator's
 *         watermark.</li>
 * </ul>
 *
 * <p>
 * Note that to notify downstream operators that a source is permanently closed and will no longer send any more elements,
 * the source should still send a {@link org.apache.flink.streaming.api.watermark.Watermark#MAX_WATERMARK} instead of
 * {@link StreamStatus#IDLE}. Stream Status elements only serve as markers for temporary status.
 */
@PublicEvolving
public final class StreamStatus extends StreamElement {

	public static final int IDLE_STATUS = -1;
	public static final int ACTIVE_STATUS = 0;

	public static final StreamStatus IDLE = new StreamStatus(IDLE_STATUS);
	public static final StreamStatus ACTIVE = new StreamStatus(ACTIVE_STATUS);

	public final int status;

	public StreamStatus(int status) {
		if (status != IDLE_STATUS && status != ACTIVE_STATUS) {
			throw new IllegalArgumentException("Invalid status value for StreamStatus; " +
				"allowed values are " + ACTIVE_STATUS + " (for ACTIVE) and " + IDLE_STATUS + " (for IDLE).");
		}

		this.status = status;
	}

	public boolean isIdle() {
		return this.status == IDLE_STATUS;
	}

	public boolean isActive() {
		return !isIdle();
	}

	public int getStatus() {
		return status;
	}

	@Override
	public boolean equals(Object o) {
		return this == o ||
			o != null && o.getClass() == StreamStatus.class && ((StreamStatus) o).status == this.status;
	}

	@Override
	public int hashCode() {
		return status;
	}

	@Override
	public String toString() {
		String statusStr = (status == ACTIVE_STATUS) ? "ACTIVE" : "IDLE";
		return "StreamStatus(" + statusStr + ")";
	}
}
