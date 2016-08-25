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

package org.apache.flink.runtime.checkpoint;

/**
 * The configuration of a checkpoint, such as whether
 * <ul>
 *     <li>The checkpoint is a savepoint</li>
 *     <li>The checkpoint must be full, or may be incremental</li>
 *     <li>The checkpoint format must be the common (cross backend) format, or may be state-backend specific</li>
 * </ul>
 */
public class CheckpointProperties {

	private final boolean isSavepoint;

	private CheckpointProperties(boolean isSavepoint) {
		this.isSavepoint = isSavepoint;
	}

	// ------------------------------------------------------------------------

	public boolean isSavepoint() {
		return isSavepoint;
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "CheckpointProperties {" +
				"isSavepoint=" + isSavepoint +
				'}';
	}

	// ------------------------------------------------------------------------

	public static CheckpointProperties forStandardSavepoint() {
		return new CheckpointProperties(true);
	}

	public static CheckpointProperties forStandardCheckpoint() {
		return new CheckpointProperties(false);
	}
}
