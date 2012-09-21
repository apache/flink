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

import java.io.IOException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.IllegalConfigurationException;
import eu.stratosphere.nephele.util.StringUtils;

public class JobGenericInputVertex extends JobInputVertex
{
	/**
	 * Class of input task.
	 */
	protected Class<? extends AbstractInputTask<?>> inputClass = null;

	/**
	 * Creates a new job input vertex with the specified name.
	 * 
	 * @param name
	 *        The name of the new job file input vertex.
	 * @param id
	 *        The ID of this vertex.
	 * @param jobGraph
	 *        The job graph this vertex belongs to.
	 */
	public JobGenericInputVertex(String name, JobVertexID id, JobGraph jobGraph) {
		super(name, id, jobGraph);
	}

	/**
	 * Creates a new job file input vertex with the specified name.
	 * 
	 * @param name
	 *        The name of the new job file input vertex.
	 * @param jobGraph
	 *        The job graph this vertex belongs to.
	 */
	public JobGenericInputVertex(String name, JobGraph jobGraph) {
		super(name, null, jobGraph);
	}

	/**
	 * Creates a new job file input vertex.
	 * 
	 * @param jobGraph
	 *        The job graph this vertex belongs to.
	 */
	public JobGenericInputVertex(JobGraph jobGraph) {
		super(null, null, jobGraph);
	}

	/**
	 * Sets the class of the vertex's input task.
	 * 
	 * @param inputClass
	 *        The class of the vertex's input task.
	 */
	public void setInputClass(Class<? extends AbstractInputTask<?>> inputClass) {
		this.inputClass = inputClass;
	}

	/**
	 * Returns the class of the vertex's input task.
	 * 
	 * @return the class of the vertex's input task or <code>null</code> if no task has yet been set
	 */
	public Class<? extends AbstractInputTask<?>> getInputClass() {
		return this.inputClass;
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void read(final Kryo kryo, final Input input) {
		super.read(kryo, input);

		// Read class
		boolean isNotNull = input.readBoolean();
		if (isNotNull) {
			// Read the name of the class and try to instantiate the class object
			ClassLoader cl = null;
			try {
				cl = LibraryCacheManager.getClassLoader(this.getJobGraph().getJobID());
			} catch (IOException ioe) {
				new RuntimeException(ioe);
			}

			// Read the name of the expected class
			final String className = input.readString();

			try {
				this.inputClass = (Class<? extends AbstractInputTask<?>>) Class.forName(className, true, cl)
					.asSubclass(AbstractInputTask.class);
			} catch (ClassNotFoundException cnfe) {
				throw new RuntimeException("Class " + className + " not found in one of the supplied jar files: "
					+ StringUtils.stringifyException(cnfe));
			} catch (ClassCastException ccex) {
				throw new RuntimeException("Class " + className + " is not a subclass of "
					+ AbstractInputTask.class.getName() + ": " + StringUtils.stringifyException(ccex));
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final Kryo kryo, final Output output) {
		super.write(kryo, output);

		// Write out the name of the class
		if (this.inputClass == null) {
			output.writeBoolean(false);
		} else {
			output.writeBoolean(true);
			output.writeString(this.inputClass.getName());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void checkConfiguration(AbstractInvokable invokable) throws IllegalConfigurationException
	{
		// see if the task itself has a valid configuration
		// because this is user code running on the master, we embed it in a catch-all block
		try {
			invokable.checkConfiguration();
		} catch (IllegalConfigurationException icex) {
			throw icex; // simply forward
		} catch (Throwable t) {
			throw new IllegalConfigurationException("Checking the invokable's configuration caused an error: "
				+ StringUtils.stringifyException(t));
		}
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
	public int getMaximumNumberOfSubtasks(AbstractInvokable invokable)
	{
		return invokable.getMaximumNumberOfSubtasks();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMinimumNumberOfSubtasks(AbstractInvokable invokable) {

		return invokable.getMinimumNumberOfSubtasks();
	}
}
