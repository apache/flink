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
import java.io.FileNotFoundException;
import java.io.IOException;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.template.AbstractFileOutputTask;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.IllegalConfigurationException;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * A JobFileOutputVertex is a specific subtype of a {@link JobOutputVertex} and is designed
 * for Nephele tasks which write data to a local or distributed file system. As every job output vertex
 * A JobFileOutputVertex must not have any further output.
 * 
 * @author warneke
 */
public class JobFileOutputVertex extends JobOutputVertex {

	/**
	 * The class of the output task.
	 */
	private Class<? extends AbstractFileOutputTask> outputClass = null;

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
	public JobFileOutputVertex(String name, JobVertexID id, JobGraph jobGraph) {
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
	public JobFileOutputVertex(String name, JobGraph jobGraph) {
		super(name, null, jobGraph);
	}

	/**
	 * Creates a new job file input vertex.
	 * 
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	public JobFileOutputVertex(JobGraph jobGraph) {
		super(null, null, jobGraph);
	}

	/**
	 * Sets the path of the file the job file input vertex's task should write to.
	 * 
	 * @param path
	 *        the path of the file the job file input vertex's task should write to
	 */
	public void setFilePath(Path path) {
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
	public void setFileOutputClass(Class<? extends AbstractFileOutputTask> outputClass) {
		this.outputClass = outputClass;
	}

	/**
	 * Returns the class of the vertex's output task.
	 * 
	 * @return the class of the vertex's output task or <code>null</code> if no task has yet been set
	 */
	public Class<? extends AbstractFileOutputTask> getFileOutputClass() {
		return this.outputClass;
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
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
				this.outputClass = (Class<? extends AbstractFileOutputTask>) Class.forName(className, true, cl);
			} catch (ClassNotFoundException cnfe) {
				throw new IOException("Class " + className + " not found in one of the supplied jar files: "
					+ StringUtils.stringifyException(cnfe));
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
		if (this.outputClass == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			StringRecord.writeString(out, this.outputClass.getName());
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
			throw new IllegalConfigurationException(this.getName() + " does not specify an output path");
		}

		// Finally, see if the task itself has a valid configuration
		invokable.checkConfiguration();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Class<? extends AbstractInvokable> getInvokableClass() {

		return this.outputClass;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumNumberOfSubtasks(AbstractInvokable invokable) {

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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMinimumNumberOfSubtasks(AbstractInvokable invokable) {

		// Delegate call to invokable
		return invokable.getMinumumNumberOfSubtasks();
	}
}
