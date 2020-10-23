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

/**
 * View over a pipelined in-memory only subpartition allowing reconnecting.
 */
public class PipelinedApproximateSubpartitionView extends PipelinedSubpartitionView {

	PipelinedApproximateSubpartitionView(PipelinedApproximateSubpartition parent, BufferAvailabilityListener listener) {
		super(parent, listener);
	}

	/**
	 * Pipelined ResultPartition relies on its subpartition view's release to decide whether the partition
	 * is ready to release. In contrast, Approximate Pipelined ResultPartition is put into the JobMaster's
	 * Partition Tracker and relies on the tracker to release partitions after the job is finished.
	 * Hence in the approximate pipelined case, no resource related to view is needed to be released.
	 */
	@Override
	public void releaseAllResources() {
		isReleased.compareAndSet(false, true);
	}
}
