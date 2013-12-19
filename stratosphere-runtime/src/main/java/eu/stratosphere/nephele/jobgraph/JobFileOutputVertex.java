/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.nephele.jobgraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;

import eu.stratosphere.configuration.IllegalConfigurationException;
import eu.stratosphere.core.fs.FileStatus;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.nephele.template.AbstractFileOutputTask;
import eu.stratosphere.nephele.template.AbstractInvokable;

/**
 * A JobFileOutputVertex is a specific subtype of a {@link AbstractJobOutputVertex} and is designed
 * for Nephele tasks which write data to a local or distributed file system. As every job output vertex
 * A JobFileOutputVertex must not have any further output.
 * 
 */
public class JobFileOutputVertex extends AbstractJobOutputVertex {

	/**
	 * The path pointing to the output file/directory.
	 */
	private Path path = null;

	/**
	 * Creates a new job file output vertex with the specified name.
	 * 
	 * @param name
	 *        the name of the new job file output vertex
	 * @param id
	 *        the ID of this vertex
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	public JobFileOutputVertex(final String name, final JobVertexID id, final JobGraph jobGraph) {
		super(name, id, jobGraph);
	}

	/**
	 * Creates a new job file output vertex with the specified name.
	 * 
	 * @param name
	 *        the name of the new job file output vertex
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	public JobFileOutputVertex(final String name, final JobGraph jobGraph) {
		super(name, null, jobGraph);
	}

	/**
	 * Creates a new job file input vertex.
	 * 
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	public JobFileOutputVertex(final JobGraph jobGraph) {
		super(null, null, jobGraph);
	}

	/**
	 * Sets the path of the file the job file input vertex's task should write to.
	 * 
	 * @param path
	 *        the path of the file the job file input vertex's task should write to
	 */
	public void setFilePath(final Path path) {
		this.path = path;
	}

	/**
	 * Returns the path of the file the job file output vertex's task should write to.
	 * 
	 * @return the path of the file the job file output vertex's task should write to or <code>null</code> if no path
	 *         has yet been set
	 */

	public Path getFilePath() {
		return this.path;
	}

	/**
	 * Sets the class of the vertex's output task.
	 * 
	 * @param outputClass
	 *        the class of the vertex's output task.
	 */
	public void setFileOutputClass(final Class<? extends AbstractFileOutputTask> outputClass) {
		this.invokableClass = outputClass;
	}

	/**
	 * Returns the class of the vertex's output task.
	 * 
	 * @return the class of the vertex's output task or <code>null</code> if no task has yet been set
	 */
	@SuppressWarnings("unchecked")
	public Class<? extends AbstractFileOutputTask> getFileOutputClass() {
		return (Class<? extends AbstractFileOutputTask>) this.invokableClass;
	}


	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);

		// Read path of the input file
		boolean isNotNull = in.readBoolean();
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
			throw new IllegalConfigurationException(this.getName() + " does not specify an output path");
		}

		super.checkConfiguration(invokable);
	}


	@Override
	public int getMaximumNumberOfSubtasks(final AbstractInvokable invokable) {

		if (this.path == null) {
			return 0;
		}

		// Check if the path is valid
		try {
			final FileSystem fs = path.getFileSystem();

			try {
				final FileStatus f = fs.getFileStatus(path);

				if (f == null) {
					return 1;
				}

				// If the path points to a directory we allow an infinity number of subtasks
				if (f.isDir()) {
					return -1;
				}
			} catch (FileNotFoundException fnfex) {
				// The exception is thrown if the requested file/directory does not exist.
				// if the degree of parallelism is > 1, we create a directory for this path
				if (getNumberOfSubtasks() > 1) {
					fs.mkdirs(path);
					return -1;
				} else {
					// a none existing file and a degree of parallelism that is one
					return 1;
				}
			}
		} catch (IOException e) {
			// any other kind of I/O exception: we assume only a degree of one here
			return 1;
		}

		return 1;
	}
}
