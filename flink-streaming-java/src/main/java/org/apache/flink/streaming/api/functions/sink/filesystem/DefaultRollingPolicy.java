/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * The default implementation of the {@link RollingPolicy}.
 *
 * <p>This policy rolls a part file if:
 * <ol>
 *     <li>there is no open part file,</li>
 * 	   <li>the current file has reached the maximum bucket size (by default 128MB),</li>
 * 	   <li>the current file is older than the roll over interval (by default 60 sec), or</li>
 * 	   <li>the current file has not been written to for more than the allowed inactivityTime (by default 60 sec).</li>
 * </ol>
 */
@PublicEvolving
public class DefaultRollingPolicy implements RollingPolicy {

	private static final long serialVersionUID = 1318929857047767030L;

	private static final long DEFAULT_INACTIVITY_INTERVAL = 60L * 1000L;

	private static final long DEFAULT_ROLLOVER_INTERVAL = 60L * 1000L;

	private static final long DEFAULT_MAX_PART_SIZE = 1024L * 1024L * 128L;

	private long maxPartSize = DEFAULT_MAX_PART_SIZE;

	private long rolloverInterval = DEFAULT_ROLLOVER_INTERVAL;

	private long inactivityInterval = DEFAULT_INACTIVITY_INTERVAL;

	public DefaultRollingPolicy() {}

	public DefaultRollingPolicy withInactivityInterval(long inactivityTime) {
		Preconditions.checkState(inactivityTime > 0L);
		this.inactivityInterval = inactivityTime;
		return this;
	}

	public DefaultRollingPolicy withMaxPartSize(long maxPartSize) {
		Preconditions.checkState(maxPartSize > 0L);
		this.maxPartSize = maxPartSize;
		return this;
	}

	public DefaultRollingPolicy withRolloverInterval(long rolloverTime) {
		Preconditions.checkState(rolloverTime > 0L);
		this.rolloverInterval = rolloverTime;
		return this;
	}

	@Override
	public boolean shouldRoll(final PartFileInfoHandler state, final long currentTime) throws IOException {
		if (!state.isOpen()) {
			return true;
		}

		if (state.getSize() > maxPartSize) {
			return true;
		}

		if (currentTime - state.getCreationTime() > rolloverInterval) {
			return true;
		}

		return currentTime - state.getLastUpdateTime() > inactivityInterval;
	}
}
