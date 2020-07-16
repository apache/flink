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

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * A buffer which encapsulates the logic of dealing with the response from the {@link CollectSinkFunction}.
 * It ignores the checkpoint related fields in the response.
 * See Java doc of {@link CollectSinkFunction} for explanation of this communication protocol.
 */
public class UncheckpointedCollectResultBuffer<T> extends AbstractCollectResultBuffer<T> {

	private final boolean failureTolerance;

	public UncheckpointedCollectResultBuffer(TypeSerializer<T> serializer, boolean failureTolerance) {
		super(serializer);
		this.failureTolerance = failureTolerance;
	}

	@Override
	protected void sinkRestarted(long lastCheckpointedOffset) {
		if (!failureTolerance) {
			// sink restarted but we do not tolerate failure
			throw new RuntimeException("Job restarted");
		}
		reset();
	}

	@Override
	protected void maintainVisibility(long currentVisiblePos, long lastCheckpointedOffset) {
		// the results are instantly visible by users
		makeResultsVisible(getOffset());
	}
}
