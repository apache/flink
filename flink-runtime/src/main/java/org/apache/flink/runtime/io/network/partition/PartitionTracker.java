/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition;

import java.util.Collection;

/**
 * Utility for tracking partitions.
 *
 * <p>This interface deliberately does not have a method to start tracking partitions, so that implementation are
 * flexible in their definitions for this method (otherwise one would end up with multiple methods, with one part likely
 * being unused).
 */
public interface PartitionTracker<K, M> {

	/**
	 * Stops the tracking of all partitions for the given key.
	 */
	Collection<PartitionTrackerEntry<K, M>> stopTrackingPartitionsFor(K key);

	/**
	 * Stops the tracking of the given partitions.
	 */
	Collection<PartitionTrackerEntry<K, M>> stopTrackingPartitions(Collection<ResultPartitionID> resultPartitionIds);

	/**
	 * Returns whether any partition is being tracked for the given key.
	 */
	boolean isTrackingPartitionsFor(K key);

	/**
	 * Returns whether the given partition is being tracked.
	 */
	boolean isPartitionTracked(ResultPartitionID resultPartitionID);
}
