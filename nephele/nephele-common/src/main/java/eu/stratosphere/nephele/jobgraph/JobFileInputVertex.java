/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.jobgraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.fs.BlockLocation;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.template.AbstractFileInputTask;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.IllegalConfigurationException;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * A JobFileInputVertex is a specific subtype of a {@link JobInputVertex} and is designed
 * for Nephele tasks which read data from a local or distributed file system. As every job input vertex
 * A JobFileInputVertex must not have any further input.
 * 
 * @author warneke
 */
public class JobFileInputVertex extends JobInputVertex {
	/**
	 * The fraction that the last split may be larger than the others.
	 */
	private static final float MAX_SPLIT_SIZE_DISCREPANCY = 1.1f;

	/**
	 * Class of input task.
	 */
	private Class<? extends AbstractFileInputTask> inputClass = null;

	/**
	 * The path pointing to the input file/directory.
	 */
	private Path path = null;

	/**
	 * Creates a new job file input vertex with the specified name.
	 * 
	 * @param name
	 *        the name of the new job file input vertex
	 * @param id
	 *        the ID of this vertex
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
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
		super(name, null, jobGraph);
	}

	/**
	 * Creates a new job file input vertex.
	 * 
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	public JobFileInputVertex(JobGraph jobGraph) {
		super(null, null, jobGraph);
	}

	/**
	 * Sets the path of the file the job file input vertex's task should read from.
	 * 
	 * @param path
	 *        the path of the file the job file input vertex's task should read from
	 */
	public void setFilePath(Path path) {
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

	/**
	 * Sets the class of the vertex's input task.
	 * 
	 * @param inputClass
	 *        the class of the vertex's input task.
	 */
	public void setFileInputClass(Class<? extends AbstractFileInputTask> inputClass) {
		this.inputClass = inputClass;
	}

	/**
	 * Returns the class of the vertex's input task.
	 * 
	 * @return the class of the vertex's input task or <code>null</code> if no task has yet been set
	 */
	public Class<? extends AbstractFileInputTask> getFileInputClass() {
		return this.inputClass;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);

		// Read class
		boolean isNotNull = in.readBoolean();
		if (isNotNull) {
			// Read the name of the class and try to instantiate the class object
			final ClassLoader cl = LibraryCacheManager.getClassLoader(this.getJobGraph().getJobID());
			if (cl == null) {
				throw new IOException("Cannot find class loader for vertex " + getID());
			}

			// Read the name of the expected class
			final String className = StringRecord.readString(in);

			try {
				this.inputClass = Class.forName(className, true, cl).asSubclass(AbstractFileInputTask.class);
			} catch (ClassNotFoundException cnfe) {
				throw new IOException("Class " + className + " not found in one of the supplied jar files: "
					+ StringUtils.stringifyException(cnfe));
			} catch (ClassCastException ccex) {
				throw new IOException("Class " + className + " is not a subclass of "
					+ AbstractFileInputTask.class.getName() + ": " + StringUtils.stringifyException(ccex));
			}
		}

		// Read path of the input file
		isNotNull = in.readBoolean();
		if (isNotNull) {
			this.path = new Path();
			this.path.read(in);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		// Write out the name of the class
		if (this.inputClass == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			StringRecord.writeString(out, this.inputClass.getName());
		}

		// Write out the path of the input file
		if (this.path == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			this.path.write(out);
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void checkConfiguration(AbstractInvokable invokable) throws IllegalConfigurationException {

		// Check if the user has specifed a path
		if (this.path == null) {
			throw new IllegalConfigurationException(this.getName() + " does not specify an input path");
		}

		// Check if the path is valid
		try {
			final FileSystem fs = path.getFileSystem();
			final FileStatus f = fs.getFileStatus(path);
			if (f == null) {
				throw new IOException(path.toString() + " led to a null object");
			}
		} catch (IOException e) {
			throw new IllegalConfigurationException("Cannot access file or directory: "
				+ StringUtils.stringifyException(e));
		}

		// Finally, see if the task itself has a valid configuration
		invokable.checkConfiguration();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Class<? extends AbstractInvokable> getInvokableClass() {

		return this.inputClass;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumNumberOfSubtasks(AbstractInvokable invokable) {

		int numberOfBlocks = -1;

		if (this.path == null) {
			return -1;
		}

		try {
			final FileSystem fs = this.path.getFileSystem();
			final FileStatus f = fs.getFileStatus(this.path);
			numberOfBlocks = fs.getNumberOfBlocks(f);

		} catch (IOException e) {
			return -1;
		}

		return (int) Math.min(numberOfBlocks, invokable.getMaximumNumberOfSubtasks());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMinimumNumberOfSubtasks(AbstractInvokable invokable) {

		return invokable.getMinimumNumberOfSubtasks();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputSplit[] getInputSplits() throws IllegalConfigurationException {
		if (this.path == null) {
			throw new IllegalConfigurationException("Cannot generate input splits, path is not set");
		}

		final int numSubtasks = getNumberOfSubtasks();
		final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>();

		// get all the files that are involved in the splits
		List<FileStatus> files = new ArrayList<FileStatus>();
		long totalLength = 0;

		try {
			final FileSystem fs = this.path.getFileSystem();
			final FileStatus pathFile = fs.getFileStatus(this.path);

			if (pathFile.isDir()) {
				// input is directory. list all contained files
				final FileStatus[] dir = fs.listStatus(this.path);
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
			final long maxSplitSize = (numSubtasks < 1) ? Long.MAX_VALUE : (totalLength / numSubtasks + (totalLength
				% numSubtasks == 0 ? 0 : 1));

			// now that we have the files, generate the splits
			for (FileStatus file : files) {
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
						FileInputSplit fis = new FileInputSplit(file.getPath(), position, splitSize, blocks[blockIndex]
							.getHosts());
						inputSplits.add(fis);

						// adjust the positions
						position += splitSize;
						bytesUnassigned -= splitSize;
					}

					// assign the last split
					if (bytesUnassigned > 0) {
						blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
						final FileInputSplit fis = new FileInputSplit(file.getPath(), position, bytesUnassigned,
							blocks[blockIndex].getHosts());
						inputSplits.add(fis);
					}
				} else {
					// special case with a file of zero bytes size
					final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, 0);
					final FileInputSplit fis = new FileInputSplit(file.getPath(), 0, 0, blocks[0].getHosts());
					inputSplits.add(fis);
				}
			}

		} catch (IOException ioe) {
			throw new IllegalConfigurationException("Cannot generate input splits from path '" + this.path.toString()
				+ "': " + StringUtils.stringifyException(ioe));
		}

		return inputSplits.toArray(new InputSplit[inputSplits.size()]);
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
	private final int getBlockIndexForPosition(BlockLocation[] blocks, long offset, long halfSplitSize, int startIndex) {
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

}
