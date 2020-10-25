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
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link SourceSplit} that represents a file, or a region of a file.
 *
 * <p>The split has an offset and an end, which defines the region of the file represented by
 * the split. For splits representing the while file, the offset is zero and the length is the
 * file size.
 *
 * <p>The split may furthermore have a "reader position", which is the checkpointed position from
 * a reader previously reading this split. This position is typically null when the split is assigned
 * from the enumerator to the readers, and is non-null when the readers checkpoint their state
 * in a file source split.
 *
 * <p>This class is {@link Serializable} for convenience. For Flink's internal serialization (both for
 * RPC and for checkpoints), the {@link FileSourceSplitSerializer} is used.
 */
@PublicEvolving
public class FileSourceSplit implements SourceSplit, Serializable {

	private static final long serialVersionUID = 1L;

	private static final String[] NO_HOSTS = StringUtils.EMPTY_STRING_ARRAY;

	/** The unique ID of the split. Unique within the scope of this source. */
	private final String id;

	/** The path of the file referenced by this split. */
	private final Path filePath;

	/** The position of the first byte in the file to process. */
	private final long offset;

	/** The number of bytes in the file to process. */
	private final long length;

	/** The names of the hosts storing this range of the file. Empty, if no host information is available. */
	private final String[] hostnames;

	/** The precise reader position in the split, to resume from. */
	@Nullable
	private final CheckpointedPosition readerPosition;

	/** The splits are frequently serialized into checkpoints.
	 * Caching the byte representation makes repeated serialization cheap.
	 * This field is used by {@link FileSourceSplitSerializer}. */
	@Nullable
	transient byte[] serializedFormCache;

	// --------------------------------------------------------------------------------------------

	/**
	 * Constructs a split with host information.
	 *
	 * @param id The unique ID of this source split.
	 * @param filePath The path to the file.
	 * @param offset The start (inclusive) of the split's rage in the file.
	 * @param length The number of bytes in the split (starting from the offset)
	 */
	public FileSourceSplit(String id, Path filePath, long offset, long length) {
		this(id, filePath, offset, length, NO_HOSTS);
	}

	/**
	 * Constructs a split with host information.
	 *
	 * @param filePath The path to the file.
	 * @param offset The start (inclusive) of the split's rage in the file.
	 * @param length The number of bytes in the split (starting from the offset)
	 * @param hostnames The hostnames of the nodes storing the split's file range.
	 */
	public FileSourceSplit(String id, Path filePath, long offset, long length, String... hostnames) {
		this(id, filePath, offset, length, hostnames, null, null);
	}

	/**
	 * Constructs a split with host information.
	 *
	 * @param filePath The path to the file.
	 * @param offset The start (inclusive) of the split's rage in the file.
	 * @param length The number of bytes in the split (starting from the offset)
	 * @param hostnames The hostnames of the nodes storing the split's file range.
	 */
	public FileSourceSplit(
			String id,
			Path filePath,
			long offset,
			long length,
			String[] hostnames,
			@Nullable CheckpointedPosition readerPosition) {
		this(id, filePath, offset, length, hostnames, readerPosition, null);
	}

	/**
	 * Package private constructor, used by the serializers to directly cache the serialized form.
	 */
	FileSourceSplit(
			String id,
			Path filePath,
			long offset,
			long length,
			String[] hostnames,
			@Nullable CheckpointedPosition readerPosition,
			@Nullable byte[] serializedForm) {

		checkArgument(offset >= 0, "offset must be >= 0");
		checkArgument(length >= 0, "length must be >= 0");
		checkNoNullHosts(hostnames);

		this.id = checkNotNull(id);
		this.filePath = checkNotNull(filePath);
		this.offset = offset;
		this.length = length;
		this.hostnames = hostnames;
		this.readerPosition = readerPosition;
		this.serializedFormCache = serializedForm;
	}

	// ------------------------------------------------------------------------
	//  split properties
	// ------------------------------------------------------------------------

	@Override
	public String splitId() {
		return id;
	}

	/**
	 * Gets the file's path.
	 */
	public Path path() {
		return filePath;
	}

	/**
	 * Returns the start of the file region referenced by this source split.
	 * The position is inclusive, the value indicates the first byte that is part of the split.
	 */
	public long offset() {
		return offset;
	}

	/**
	 * Returns the number of bytes in the file region described by this source split.
	 */
	public long length() {
		return length;
	}

	/**
	 * Gets the hostnames of the nodes storing the file range described by this split.
	 * The returned array is empty, if no host information is available.
	 *
	 * <p>Host information is typically only available on specific file systems, like HDFS.
	 */
	public String[] hostnames() {
		return hostnames;
	}

	/**
	 * Gets the (checkpointed) position of the reader, if set.
	 * This value is typically absent for splits when assigned from the enumerator to the readers,
	 * and present when the splits are recovered from a checkpoint.
	 */
	public Optional<CheckpointedPosition> getReaderPosition() {
		return Optional.ofNullable(readerPosition);
	}

	/**
	 * Creates a copy of this split where the checkpointed position is replaced by the
	 * given new position.
	 *
	 * <p><b>IMPORTANT:</b> Subclasses that add additional information to the split
	 * must override this method to return that subclass type. This contract is enforced by
	 * checks in the file source implementation.
	 * We did not try to enforce this contract via generics in this split class, because
	 * it leads to very ugly and verbose use of generics.
	 */
	public FileSourceSplit updateWithCheckpointedPosition(@Nullable CheckpointedPosition position) {
		return new FileSourceSplit(id, filePath, offset, length, hostnames, position);
	}

	// ------------------------------------------------------------------------
	//  utils
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		final String hosts = hostnames.length == 0 ? "(no host info)" : " hosts=" + Arrays.toString(hostnames);
		return String.format("FileSourceSplit: %s [%d, %d) %s ID=%s position=%s",
				filePath, offset, offset + length, hosts, id, readerPosition);
	}

	private static void checkNoNullHosts(String[] hosts) {
		checkNotNull(hosts, "hostnames array must not be null");
		for (String host : hosts) {
			checkArgument(host != null, "the hostnames must not contain null entries");
		}
	}
}
