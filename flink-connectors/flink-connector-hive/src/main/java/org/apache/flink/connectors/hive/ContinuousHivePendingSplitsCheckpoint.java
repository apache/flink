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

package org.apache.flink.connectors.hive;

import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connectors.hive.read.HiveSourceSplit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * The checkpoint of current state of continuous hive source reading.
 */
public class ContinuousHivePendingSplitsCheckpoint extends PendingSplitsCheckpoint<HiveSourceSplit> {

	private final Comparable<?> currentReadOffset;
	private final Collection<List<String>> seenPartitionsSinceOffset;

	public ContinuousHivePendingSplitsCheckpoint(
			Collection<HiveSourceSplit> splits,
			Comparable<?> currentReadOffset,
			Collection<List<String>> seenPartitionsSinceOffset) {
		super(new ArrayList<>(splits), Collections.emptyList());
		this.currentReadOffset = currentReadOffset;
		this.seenPartitionsSinceOffset = Collections.unmodifiableCollection(new ArrayList<>(seenPartitionsSinceOffset));
	}

	public Comparable<?> getCurrentReadOffset() {
		return currentReadOffset;
	}

	public Collection<List<String>> getSeenPartitionsSinceOffset() {
		return seenPartitionsSinceOffset;
	}
}
