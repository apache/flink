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

package org.apache.flink.streaming.api.functions.sink.filesystem.rolling.policies;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
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
public final class DefaultRollingPolicy<BucketID> implements RollingPolicy<BucketID> {

	private static final long serialVersionUID = 1L;

	private static final long DEFAULT_INACTIVITY_INTERVAL = 60L * 1000L;

	private static final long DEFAULT_ROLLOVER_INTERVAL = 60L * 1000L;

	private static final long DEFAULT_MAX_PART_SIZE = 1024L * 1024L * 128L;

	private final long partSize;

	private final long rolloverInterval;

	private final long inactivityInterval;

	/**
	 * Private constructor to avoid direct instantiation.
	 */
	private DefaultRollingPolicy(long partSize, long rolloverInterval, long inactivityInterval) {
		Preconditions.checkArgument(partSize > 0L);
		Preconditions.checkArgument(rolloverInterval > 0L);
		Preconditions.checkArgument(inactivityInterval > 0L);

		this.partSize = partSize;
		this.rolloverInterval = rolloverInterval;
		this.inactivityInterval = inactivityInterval;
	}

	@Override
	public boolean shouldRollOnCheckpoint(PartFileInfo<BucketID> partFileState) {
		return false;
	}

	@Override
	public boolean shouldRollOnEvent(PartFileInfo<BucketID> partFileState) throws IOException {
		if (partFileState == null) {
			// this means that there is no currently open part file.
			return true;
		}

		return partFileState.getSize() > partSize;
	}

	@Override
	public boolean shouldRollOnProcessingTime(final PartFileInfo<BucketID> partFileState, final long currentTime) {
		if (partFileState == null) {
			// this means that there is no currently open part file.
			return true;
		}

		if (currentTime - partFileState.getCreationTime() > rolloverInterval) {
			return true;
		}

		return currentTime - partFileState.getLastUpdateTime() > inactivityInterval;
	}

	/**
	 * Initiates the instantiation of a {@code DefaultRollingPolicy}.
	 * To finalize it and have the actual policy, call {@code .create()}.
	 */
	public static DefaultRollingPolicy.PolicyBuilder create() {
		return new DefaultRollingPolicy.PolicyBuilder();
	}

	/**
	 * A helper class that holds the configuration properties for the {@link DefaultRollingPolicy}.
	 */
	@PublicEvolving
	public static final class PolicyBuilder {

		private long partSize = DEFAULT_MAX_PART_SIZE;

		private long rolloverInterval = DEFAULT_ROLLOVER_INTERVAL;

		private long inactivityInterval = DEFAULT_INACTIVITY_INTERVAL;

		private PolicyBuilder() {}

		/**
		 * Sets the part size above which a part file will have to roll.
		 * @param size the allowed part size.
		 */
		public DefaultRollingPolicy.PolicyBuilder withMaxPartSize(long size) {
			Preconditions.checkState(size > 0L);
			this.partSize = size;
			return this;
		}

		/**
		 * Sets the interval of allowed inactivity after which a part file will have to roll.
		 * @param interval the allowed inactivity interval.
		 */
		public DefaultRollingPolicy.PolicyBuilder withInactivityInterval(long interval) {
			Preconditions.checkState(interval > 0L);
			this.inactivityInterval = interval;
			return this;
		}

		/**
		 * Sets the max time a part file can stay open before having to roll.
		 * @param interval the desired rollover interval.
		 */
		public DefaultRollingPolicy.PolicyBuilder withRolloverInterval(long interval) {
			Preconditions.checkState(interval > 0L);
			this.rolloverInterval = interval;
			return this;
		}

		/**
		 * Creates the actual policy.
		 */
		public <BucketID> DefaultRollingPolicy<BucketID> build() {
			return new DefaultRollingPolicy<>(partSize, rolloverInterval, inactivityInterval);
		}
	}
}
