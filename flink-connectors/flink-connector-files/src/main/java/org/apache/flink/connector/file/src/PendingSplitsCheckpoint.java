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

package org.apache.flink.connector.file.src;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A checkpoint of the current state of the containing the currently pending splits that are not yet assigned.
 */
@PublicEvolving
public final class PendingSplitsCheckpoint {

	/** The splits in the checkpoint. */
	private final Collection<FileSourceSplit> splits;

	/** The paths that are no longer in the enumerator checkpoint, but have been processed
	 * before and should this be ignored. Relevant only for sources in continuous monitoring mode. */
	private final Collection<Path> alreadyProcessedPaths;

	/** The cached byte representation from the last serialization step. This helps to avoid
	 * paying repeated serialization cost for the same checkpoint object. This field is used
	 * by {@link PendingSplitsCheckpointSerializer}. */
	@Nullable
	byte[] serializedFormCache;

	private PendingSplitsCheckpoint(Collection<FileSourceSplit> splits, Collection<Path> alreadyProcessedPaths) {
		this.splits = Collections.unmodifiableCollection(splits);
		this.alreadyProcessedPaths = Collections.unmodifiableCollection(alreadyProcessedPaths);
	}

	// ------------------------------------------------------------------------

	public Collection<FileSourceSplit> getSplits() {
		return splits;
	}

	public Collection<Path> getAlreadyProcessedPaths() {
		return alreadyProcessedPaths;
	}

	// ------------------------------------------------------------------------
	//  factories
	// ------------------------------------------------------------------------

	public static PendingSplitsCheckpoint fromCollectionSnapshot(Collection<FileSourceSplit> splits) {
		checkNotNull(splits);

		// create a copy of the collection to make sure this checkpoint is immutable
		final Collection<FileSourceSplit> copy = new ArrayList<>(splits);
		return new PendingSplitsCheckpoint(copy, Collections.emptySet());
	}

	public static PendingSplitsCheckpoint fromCollectionSnapshot(
			Collection<FileSourceSplit> splits,
			Collection<Path> alreadyProcessedPaths) {
		checkNotNull(splits);

		// create a copy of the collection to make sure this checkpoint is immutable
		final Collection<FileSourceSplit> splitsCopy = new ArrayList<>(splits);
		final Collection<Path> pathsCopy = new ArrayList<>(alreadyProcessedPaths);

		return new PendingSplitsCheckpoint(splitsCopy, pathsCopy);
	}

	static PendingSplitsCheckpoint reusingCollection(Collection<FileSourceSplit> splits, Collection<Path> alreadyProcessedPaths) {
		return new PendingSplitsCheckpoint(splits, alreadyProcessedPaths);
	}
}
