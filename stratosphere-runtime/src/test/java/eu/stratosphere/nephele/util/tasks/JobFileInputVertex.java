/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.util.tasks;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.core.fs.BlockLocation;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.FileStatus;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.core.io.InputSplit;
import eu.stratosphere.nephele.jobgraph.AbstractJobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobVertexID;


public final class JobFileInputVertex extends AbstractJobInputVertex {

	/**
	 * The fraction that the last split may be larger than the others.
	 */
	private static final float MAX_SPLIT_SIZE_DISCREPANCY = 1.1f;
	
	/**
	 * The path pointing to the input file/directory.
	 */
	private Path path;


	public JobFileInputVertex(String name, JobVertexID id, JobGraph jobGraph) {
		super(name, id, jobGraph);
	}
	
	/**
	 * Creates a new job file input vertex with the specified name.
	 * 
	 * @param name
	 *        the name of the new job file input vertex
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	public JobFileInputVertex(String name, JobGraph jobGraph) {
		this(name, null, jobGraph);
	}

	/**
	 * Creates a new job file input vertex.
	 * 
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	public JobFileInputVertex(JobGraph jobGraph) {
		this(null, jobGraph);
	}

	/**
	 * Sets the path of the file the job file input vertex's task should read from.
	 * 
	 * @param path
	 *        the path of the file the job file input vertex's task should read from
	 */
	public void setFilePath(final Path path) {
		this.path = path;
	}

	/**
	 * Returns the path of the file the job file input vertex's task should read from.
	 * 
	 * @return the path of the file the job file input vertex's task should read from or <code>null</code> if no path
	 *         has yet been set
	 */
	public Path getFilePath() {
		return this.path;
	}

	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);

		// Read path of the input file
		final boolean isNotNull = in.readBoolean();
		if (isNotNull) {
			this.path = new Path();
			this.path.read(in);
		}
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);

		// Write out the path of the input file
		if (this.path == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			this.path.write(out);
		}
	}

	// --------------------------------------------------------------------------------------------


	@Override
	public InputSplit[] getInputSplits(int minNumSplits) throws Exception {
		final Path path = this.path;
		final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>();

		// get all the files that are involved in the splits
		final List<FileStatus> files = new ArrayList<FileStatus>();
		long totalLength = 0;

		final FileSystem fs = path.getFileSystem();
		final FileStatus pathFile = fs.getFileStatus(path);

		if (pathFile.isDir()) {
			// input is directory. list all contained files
			final FileStatus[] dir = fs.listStatus(path);
			for (int i = 0; i < dir.length; i++) {
				if (!dir[i].isDir()) {
					files.add(dir[i]);
					totalLength += dir[i].getLen();
				}
			}

		} else {
			files.add(pathFile);
			totalLength += pathFile.getLen();
		}

		final long minSplitSize = 1;
		final long maxSplitSize = (minNumSplits < 1) ? Long.MAX_VALUE : (totalLength / minNumSplits +
					(totalLength % minNumSplits == 0 ? 0 : 1));

		// now that we have the files, generate the splits
		int splitNum = 0;
		for (final FileStatus file : files) {

			final long len = file.getLen();
			final long blockSize = file.getBlockSize();

			final long splitSize = Math.max(minSplitSize, Math.min(maxSplitSize, blockSize));
			final long halfSplit = splitSize >>> 1;

			final long maxBytesForLastSplit = (long) (splitSize * MAX_SPLIT_SIZE_DISCREPANCY);

			if (len > 0) {

				// get the block locations and make sure they are in order with respect to their offset
				final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
				Arrays.sort(blocks);

				long bytesUnassigned = len;
				long position = 0;

				int blockIndex = 0;

				while (bytesUnassigned > maxBytesForLastSplit) {
					// get the block containing the majority of the data
					blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
					// create a new split
					final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position, splitSize,
						blocks[blockIndex]
							.getHosts());
					inputSplits.add(fis);

					// adjust the positions
					position += splitSize;
					bytesUnassigned -= splitSize;
				}

				// assign the last split
				if (bytesUnassigned > 0) {
					blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
					final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position,
						bytesUnassigned,
						blocks[blockIndex].getHosts());
					inputSplits.add(fis);
				}
			} else {
				// special case with a file of zero bytes size
				final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, 0);
				String[] hosts;
				if (blocks.length > 0) {
					hosts = blocks[0].getHosts();
				} else {
					hosts = new String[0];
				}
				final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), 0, 0, hosts);
				inputSplits.add(fis);
			}
		}

		return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
	}

	/**
	 * Retrieves the index of the <tt>BlockLocation</tt> that contains the part of the file described by the given
	 * offset.
	 * 
	 * @param blocks
	 *        The different blocks of the file. Must be ordered by their offset.
	 * @param offset
	 *        The offset of the position in the file.
	 * @param startIndex
	 *        The earliest index to look at.
	 * @return The index of the block containing the given position.
	 */
	private final int getBlockIndexForPosition(final BlockLocation[] blocks, final long offset,
			final long halfSplitSize, final int startIndex) {
		
		// go over all indexes after the startIndex
		for (int i = startIndex; i < blocks.length; i++) {
			long blockStart = blocks[i].getOffset();
			long blockEnd = blockStart + blocks[i].getLength();

			if (offset >= blockStart && offset < blockEnd) {
				// got the block where the split starts
				// check if the next block contains more than this one does
				if (i < blocks.length - 1 && blockEnd - offset < halfSplitSize) {
					return i + 1;
				} else {
					return i;
				}
			}
		}
		throw new IllegalArgumentException("The given offset is not contained in the any block.");
	}


	@Override
	public Class<FileInputSplit> getInputSplitType() {
		return FileInputSplit.class;
	}
}