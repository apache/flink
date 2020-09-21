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

package org.apache.flink.connector.file.src.enumerate;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.compression.StandardDeCompressors;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This {@code FileEnumerator} enumerates all files under the given paths recursively,
 * and creates a separate split for each file block.
 *
 * <p>Please note that file blocks are only exposed by some file systems, such as HDFS. File systems
 * that do not expose block information will not create multiple file splits per file, but keep
 * the files as one source split.
 *
 * <p>Files with suffixes corresponding to known compression formats (for example '.gzip', '.bz2', ...)
 * will not be split. See {@link StandardDeCompressors} for a list of known formats and suffixes.
 *
 * <p>The default instantiation of this enumerator filters files with the common hidden file prefixes
 * '.' and '_'. A custom file filter can be specified.
 */
@PublicEvolving
public class BlockSplittingRecursiveEnumerator extends NonSplittingRecursiveEnumerator {

	private static final Logger LOG = LoggerFactory.getLogger(BlockSplittingRecursiveEnumerator.class);

	private final String[] nonSplittableFileSuffixes;

	/**
	 * Creates a new enumerator that enumerates all files except hidden files.
	 * Hidden files are considered files where the filename starts with '.' or with '_'.
	 *
	 * <p>The enumerator does not split files that have a suffix corresponding to a known
	 * compression format (for example '.gzip', '.bz2', '.xy', '.zip', ...).
	 * See {@link StandardDeCompressors} for details.
	 */
	public BlockSplittingRecursiveEnumerator() {
		this(new DefaultFileFilter(), StandardDeCompressors.getCommonSuffixes().toArray(new String[0]));
	}

	/**
	 * Creates a new enumerator that uses the given predicate as a filter
	 * for file paths, and avoids splitting files with the given extension (typically
	 * to avoid splitting compressed files).
	 */
	public BlockSplittingRecursiveEnumerator(
			final Predicate<Path> fileFilter,
			final String[] nonSplittableFileSuffixes) {
		super(fileFilter);
		this.nonSplittableFileSuffixes = checkNotNull(nonSplittableFileSuffixes);
	}

	protected void convertToSourceSplits(
			final FileStatus file,
			final FileSystem fs,
			final List<FileSourceSplit> target) throws IOException {

		if (!isFileSplittable(file.getPath())) {
			super.convertToSourceSplits(file, fs, target);
			return;
		}

		final BlockLocation[] blocks = getBlockLocationsForFile(file, fs);
		if (blocks == null) {
			target.add(new FileSourceSplit(getNextId(), file.getPath(), 0L, file.getLen()));
		} else {
			for (BlockLocation block : blocks) {
				target.add(new FileSourceSplit(
						getNextId(),
						file.getPath(),
						block.getOffset(),
						block.getLength(),
						block.getHosts()));
			}
		}
	}

	protected boolean isFileSplittable(Path filePath) {
		if (nonSplittableFileSuffixes.length == 0) {
			return true;
		}

		final String path = filePath.getPath();
		for (String suffix : nonSplittableFileSuffixes) {
			if (path.endsWith(suffix)) {
				return false;
			}
		}
		return true;
	}

	@Nullable
	private static BlockLocation[] getBlockLocationsForFile(FileStatus file, FileSystem fs) throws IOException {
		final long len = file.getLen();

		final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
		if (blocks == null || blocks.length == 0) {
			return null;
		}

		// a cheap check whether we have all blocks. we don't check whether the blocks fully cover the
		// file (too expensive) but make some sanity checks to catch early the common cases where incorrect
		// bloc info is returned by the implementation.

		long totalLen = 0L;
		for (BlockLocation block : blocks) {
			totalLen += block.getLength();
		}
		if (totalLen != len) {
			LOG.warn("Block lengths do not match file length for {}. File length is {}, blocks are {}",
					file.getPath(), len, Arrays.toString(blocks));
			return null;
		}

		return blocks;
	}
}
