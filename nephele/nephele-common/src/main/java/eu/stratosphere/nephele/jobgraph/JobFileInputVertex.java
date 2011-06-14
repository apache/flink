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

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.template.AbstractFileInputTask;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.IllegalConfigurationException;
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
		}
		catch (IOException e) {
			throw new IllegalConfigurationException("Cannot access file or directory: "
				+ StringUtils.stringifyException(e));
		}
		
		// register the path in the configuration
		invokable.getRuntimeConfiguration().setString(AbstractFileInputTask.INPUT_PATH_CONFIG_KEY, this.path.toString());

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
}
