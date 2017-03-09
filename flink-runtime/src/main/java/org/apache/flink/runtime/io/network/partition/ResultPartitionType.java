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


public enum ResultPartitionType {

	BLOCKING(PersistentType.LOCAL, false, false),

	PIPELINED(PersistentType.NON, true, true),

	PIPELINED_PERSISTENT(PersistentType.LOCAL, true, true),

	DFS(PersistentType.DFS, false, false);

	public enum PersistentType {
		NON,
		LOCAL,
		DFS
	}

	/** Does the partition live longer than the consuming task? */
	private final PersistentType persistentType;

	/** Can the partition be consumed while being produced? */
	private final boolean isPipelined;

	/** Does the partition produce back pressure when not consumed? */
	private final boolean hasBackPressure;

	/**
	 * Specifies the behaviour of an intermediate result partition at runtime.
	 */
	ResultPartitionType(PersistentType persistentType, boolean isPipelined, boolean hasBackPressure) {
		this.persistentType = persistentType;
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

	public PersistentType getPersistentType() {
		return persistentType;
	}
}
