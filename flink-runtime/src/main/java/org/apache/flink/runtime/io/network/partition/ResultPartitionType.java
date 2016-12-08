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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.configuration.ConfigConstants;

public enum ResultPartitionType {

	BLOCKING(true, false, false),

	PIPELINED(false, true, true),

	/**
	 * Pipelined partitions with a bounded queue for buffers. The queue size is
	 * is configured via {@link ConfigConstants#DEFAULT_NETWORK_PIPELINED_BOUNDED_QUEUE_LENGTH}.
	 *
	 * For streaming jobs a fixed limit should help avoid that single downstream
	 * operators get a disproportionally large backlog. For batch jobs, it will
	 * be best to keep this unlimited ({@link #PIPELINED} and let the local buffer
	 * pools limit how much is queued.
	 */
	PIPELINED_BOUNDED(false, true, true);

	/** Does the partition live longer than the consuming task? */
	private final boolean isPersistent;

	/** Can the partition be consumed while being produced? */
	private final boolean isPipelined;

	/** Does the partition produce back pressure when not consumed? */
	private final boolean hasBackPressure;

	/**
	 * Specifies the behaviour of an intermediate result partition at runtime.
	 */
	ResultPartitionType(boolean isPersistent, boolean isPipelined, boolean hasBackPressure) {
		this.isPersistent = isPersistent;
		this.isPipelined = isPipelined;
		this.hasBackPressure = hasBackPressure;
	}

	public boolean hasBackPressure() {
		return hasBackPressure;
	}

	public boolean isBlocking() {
		return !isPipelined;
	}

	public boolean isPipelined() {
		return isPipelined;
	}

	public boolean isPersistent() {
		return isPersistent;
	}
}
