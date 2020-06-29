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
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link SourceSplit} that represents a file, or a region of a file.
 */
@PublicEvolving
public class FileSourceSplit implements SourceSplit, Serializable {

	private static final long serialVersionUID = 1L;

	private static final String[] NO_HOSTS = new String[0];

	/** The unique ID of the split. Unique within the scope of this source. */
	private final String id;

	/** The path of the file referenced by this split. */
	private final Path filePath;

	/** The position of the first byte in the file to process. */
	private final long offset;

	/** The number of bytes in the file to process. */
	private final long length;

	/** The number of records to be skipped from the beginning of the split.
	 * This is for file formats that cannot pinpoint every exact record position via an offset,
	 * due to read buffers or bulk encoding or compression. */
	private final long skippedRecordCount;

	/** The names of the hosts storing this range of the file. Empty, if no host information is available. */
	private final String[] hostnames;

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
		this(id, filePath, offset, length, hostnames, null, 0L);
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
			@Nullable byte[] serializedForm,
			long skippedRecordCount) {

		checkArgument(offset >= 0, "offset must be >= 0");
		checkArgument(length >= 0, "length must be >= 0");
		checkArgument(skippedRecordCount >= 0, "skippedRecordCount must be >= 0");
		checkNoNullHosts(hostnames);

		this.id = checkNotNull(id);
		this.filePath = checkNotNull(filePath);
		this.offset = offset;
		this.length = length;
		this.hostnames = checkNotNull(hostnames);
		this.serializedFormCache = serializedForm;
		this.skippedRecordCount = skippedRecordCount;
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
	 * Gets the number of records to be skipped from the beginning of the split.
	 * This is for file formats that cannot pinpoint every exact record position via an offset,
	 * due to read buffers or bulk encoding or compression.
	 *
	 * <p>This value is typically zero for new splits, and can be non-zero for splits that represent
	 * intermediate progress of a reader in a checkpoint.
	 */
	public long skippedRecordCount() {
		return skippedRecordCount;
	}

	// ------------------------------------------------------------------------
	//  utils
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		final String hosts = hostnames.length == 0 ? "(no host info)" : " hosts=" + Arrays.toString(hostnames);
		return String.format("FileSourceSplit: %s [%d, %d) %s ID=%s skippedRecordCount=%d",
				filePath, offset, offset + length, hosts, id, skippedRecordCount);
	}

	private static void checkNoNullHosts(String[] hosts) {
		for (String host : hosts) {
			checkArgument(host != null, "the hostnames must not contain null entries");
		}
	}
}
