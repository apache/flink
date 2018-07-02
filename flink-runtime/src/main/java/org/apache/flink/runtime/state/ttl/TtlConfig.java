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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Preconditions;

/**
 * Configuration of state TTL logic.
 * TODO: builder
 */
public class TtlConfig {
	/**
	 * This option value configures when to update last access timestamp which prolongs state TTL.
	 */
	public enum TtlUpdateType {
		/** TTL is disabled. State does not expire. */
		Disabled,
		/** Last access timestamp is initialised when state is created and updated on every write operation. */
		OnCreateAndWrite,
		/** The same as <code>OnCreateAndWrite</code> but also updated on read. */
		OnReadAndWrite
	}

	/**
	 * This option configures whether expired user value can be returned or not.
	 */
	public enum TtlStateVisibility {
		/** Return expired user value if it is not cleaned up yet. */
		ReturnExpiredIfNotCleanedUp,
		/** Never return expired user value. */
		NeverReturnExpired
	}

	/**
	 * This option configures time scale to use for ttl.
	 */
	public enum TtlTimeCharacteristic {
		/** Processing time, see also <code>TimeCharacteristic.ProcessingTime</code>. */
		ProcessingTime
	}

	private final TtlUpdateType ttlUpdateType;
	private final TtlStateVisibility stateVisibility;
	private final TtlTimeCharacteristic timeCharacteristic;
	private final Time ttl;

	public TtlConfig(
		TtlUpdateType ttlUpdateType,
		TtlStateVisibility stateVisibility,
		TtlTimeCharacteristic timeCharacteristic,
		Time ttl) {
		Preconditions.checkNotNull(ttlUpdateType);
		Preconditions.checkNotNull(stateVisibility);
		Preconditions.checkNotNull(timeCharacteristic);
		Preconditions.checkNotNull(ttl);
		Preconditions.checkArgument(ttl.toMilliseconds() > 0,
			"TTL is expected to be positive");
		this.ttlUpdateType = ttlUpdateType;
		this.stateVisibility = stateVisibility;
		this.timeCharacteristic = timeCharacteristic;
		this.ttl = ttl;
	}

	public TtlUpdateType getTtlUpdateType() {
		return ttlUpdateType;
	}

	public TtlStateVisibility getStateVisibility() {
		return stateVisibility;
	}

	public Time getTtl() {
		return ttl;
	}

	public TtlTimeCharacteristic getTimeCharacteristic() {
		return timeCharacteristic;
	}
}
