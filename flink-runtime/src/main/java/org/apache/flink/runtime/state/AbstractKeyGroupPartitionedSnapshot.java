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

package org.apache.flink.runtime.state;

import org.apache.flink.core.memory.DataOutputView;

import javax.annotation.Nonnull;

import java.io.IOException;


/**
 * Abstract base class for implementations of
 * {@link org.apache.flink.runtime.state.StateSnapshot.KeyGroupPartitionedSnapshot} based on the result of a
 * {@link AbstractKeyGroupPartitioner}.
 *
 * @param <T> type of the written elements.
 */
public abstract class AbstractKeyGroupPartitionedSnapshot<T> implements StateSnapshot.KeyGroupPartitionedSnapshot {

	/** The partitioning result to be written by key-group. */
	@Nonnull
	private final AbstractKeyGroupPartitioner.PartitioningResult<T> partitioningResult;

	public AbstractKeyGroupPartitionedSnapshot(
		@Nonnull AbstractKeyGroupPartitioner.PartitioningResult<T> partitioningResult) {
		this.partitioningResult = partitioningResult;
	}

	@Override
	public void writeMappingsInKeyGroup(@Nonnull DataOutputView dov, int keyGroupId) throws IOException {

		final T[] groupedOut = partitioningResult.getPartitionedElements();

		int startOffset = partitioningResult.getKeyGroupStartOffsetInclusive(keyGroupId);
		int endOffset = partitioningResult.getKeyGroupEndOffsetExclusive(keyGroupId);

		// write number of mappings in key-group
		dov.writeInt(endOffset - startOffset);

		// write mappings
		for (int i = startOffset; i < endOffset; ++i) {
			if(groupedOut[i] == null) {
				throw new IllegalStateException();
			}
			writeElement(groupedOut[i], dov);
			groupedOut[i] = null; // free asap for GC
		}
	}

	/**
	 * This method defines how to write a single element to the output.
	 *
	 * @param element the element to be written.
	 * @param dov the output view to write the element.
	 * @throws IOException on write-related problems.
	 */
	protected abstract void writeElement(@Nonnull T element, @Nonnull DataOutputView dov) throws IOException;
}

