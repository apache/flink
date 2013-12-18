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

import eu.stratosphere.configuration.IllegalConfigurationException;
import eu.stratosphere.core.fs.FileStatus;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.nephele.template.AbstractFileInputTask;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.util.StringUtils;

/**
 * A JobFileInputVertex is a specific subtype of a {@link AbstractJobInputVertex} and is designed
 * for Nephele tasks which read data from a local or distributed file system. As every job input vertex
 * A JobFileInputVertex must not have any further input.
 * 
 * @author warneke
 */
public final class JobFileInputVertex extends AbstractJobInputVertex {

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
	public JobFileInputVertex(final String name, final JobVertexID id, final JobGraph jobGraph) {
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
	public JobFileInputVertex(final String name, final JobGraph jobGraph) {
		super(name, null, jobGraph);
	}

	/**
	 * Creates a new job file input vertex.
	 * 
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	public JobFileInputVertex(final JobGraph jobGraph) {
		super(null, null, jobGraph);
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

	/**
	 * Sets the class of the vertex's input task.
	 * 
	 * @param inputClass
	 *        the class of the vertex's input task.
	 */
	public void setFileInputClass(final Class<? extends AbstractFileInputTask> inputClass) {
		this.invokableClass = inputClass;
	}

	/**
	 * Returns the class of the vertex's input task.
	 * 
	 * @return the class of the vertex's input task or <code>null</code> if no task has yet been set
	 */
	@SuppressWarnings("unchecked")
	public Class<? extends AbstractFileInputTask> getFileInputClass() {
		return (Class<? extends AbstractFileInputTask>) this.invokableClass;
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


	@Override
	public void checkConfiguration(final AbstractInvokable invokable) throws IllegalConfigurationException {

		// Check if the user has specified a path
		if (this.path == null) {
			throw new IllegalConfigurationException(this.getName() + " does not specify an input path");
		}

		// Check if the path is valid
		try {
			final FileSystem fs = this.path.getFileSystem();
			final FileStatus f = fs.getFileStatus(this.path);
			if (f == null) {
				throw new IOException(this.path.toString() + " led to a null object");
			}
		} catch (IOException e) {
			throw new IllegalConfigurationException("Cannot access file or directory: "
				+ StringUtils.stringifyException(e));
		}

		// register the path in the configuration
		invokable.getTaskConfiguration()
			.setString(AbstractFileInputTask.INPUT_PATH_CONFIG_KEY, this.path.toString());

		// Finally, see if the task itself has a valid configuration
		super.checkConfiguration(invokable);
	}


	@Override
	public int getMaximumNumberOfSubtasks(final AbstractInvokable invokable) {

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
}
