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

package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.annotation.Internal;

/**
 * Grouped settings for externalized checkpoints.
 */
@Internal
public class ExternalizedCheckpointSettings implements java.io.Serializable {

	private static final long serialVersionUID = -6271691851124392955L;

	private static final ExternalizedCheckpointSettings NONE = new ExternalizedCheckpointSettings(false, false);

	/** Flag indicating whether checkpoints should be externalized. */
	private final boolean externalizeCheckpoints;

	/** Flag indicating whether externalized checkpoints should delete on cancellation. */
	private final boolean deleteOnCancellation;

	private ExternalizedCheckpointSettings(boolean externalizeCheckpoints, boolean deleteOnCancellation) {
		this.externalizeCheckpoints = externalizeCheckpoints;
		this.deleteOnCancellation = deleteOnCancellation;
	}

	/**
	 * Returns <code>true</code> if checkpoints should be externalized.
	 *
	 * @return <code>true</code> if checkpoints should be externalized.
	 */
	public boolean externalizeCheckpoints() {
		return externalizeCheckpoints;
	}

	/**
	 * Returns <code>true</code> if externalized checkpoints should be deleted on cancellation.
	 *
	 * @return <code>true</code> if externalized checkpoints should be deleted on cancellation.
	 */
	public boolean deleteOnCancellation() {
		return deleteOnCancellation;
	}

	public static ExternalizedCheckpointSettings externalizeCheckpoints(boolean deleteOnCancellation) {
		return new ExternalizedCheckpointSettings(true, deleteOnCancellation);
	}

	public static ExternalizedCheckpointSettings none() {
		return NONE;
	}

}
