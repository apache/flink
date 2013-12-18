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
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.util.StringUtils;

/**
 * A JobGenericOutputVertex is a specific subtype of a {@link JobOutputVertex} and is designed
 * for Nephele tasks which sink data in a not further specified way. As every job output vertex,
 * a JobGenericOutputVertex must not have any further output.
 * 
 * @author warneke
 */
public class JobGenericOutputVertex extends JobOutputVertex {

	/**
	 * The class of the output task.
	 */
	protected Class<? extends AbstractOutputTask> outputClass = null;


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
	public JobGenericOutputVertex(String name, JobVertexID id, JobGraph jobGraph) {
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
	public JobGenericOutputVertex(String name, JobGraph jobGraph) {
		super(name, null, jobGraph);
	}

	/**
	 * Creates a new job file input vertex.
	 * 
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	public JobGenericOutputVertex(JobGraph jobGraph) {
		super(null, null, jobGraph);
	}

	/**
	 * Sets the class of the vertex's output task.
	 * 
	 * @param outputClass The class of the vertex's output task.
	 */
	public void setOutputClass(Class<? extends AbstractOutputTask> outputClass) {
		this.outputClass = outputClass;
	}

	/**
	 * Returns the class of the vertex's output task.
	 * 
	 * @return The class of the vertex's output task or <code>null</code> if no task has yet been set.
	 */
	public Class<? extends AbstractOutputTask> getOutputClass() {
		return this.outputClass;
	}


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
				this.outputClass = Class.forName(className, true, cl).asSubclass(AbstractOutputTask.class);
			}
			catch (ClassNotFoundException cnfe) {
				throw new IOException("Class " + className + " not found in one of the supplied jar files: "
					+ StringUtils.stringifyException(cnfe));
			}
			catch (ClassCastException ccex) {
				throw new IOException("Class " + className + " is not a subclass of "
					+ AbstractOutputTask.class.getName() + ": " + StringUtils.stringifyException(ccex));
			}
		}
	}


	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		// Write out the name of the class
		if (this.outputClass == null) {
			out.writeBoolean(false);
		}
		else {
			out.writeBoolean(true);
			StringRecord.writeString(out, this.outputClass.getName());
		}
	}


	@Override
	public void checkConfiguration(AbstractInvokable invokable) throws IllegalConfigurationException
	{
		// see if the task itself has a valid configuration
		// because this is user code running on the master, we embed it in a catch-all block
		try {
			invokable.checkConfiguration();
		}
		catch (IllegalConfigurationException icex) {
			throw icex; // simply forward
		}
		catch (Throwable t) {
			throw new IllegalConfigurationException("Checking the invokable's configuration caused an error: " 
				+ StringUtils.stringifyException(t));
		}
	}


	@Override
	public Class<? extends AbstractInvokable> getInvokableClass() {

		return this.outputClass;
	}


	@Override
	public int getMaximumNumberOfSubtasks(AbstractInvokable invokable)
	{
		// Delegate call to invokable
		return invokable.getMaximumNumberOfSubtasks();
	}


	@Override
	public int getMinimumNumberOfSubtasks(AbstractInvokable invokable)
	{
		// Delegate call to invokable
		return invokable.getMinimumNumberOfSubtasks();
	}
}
