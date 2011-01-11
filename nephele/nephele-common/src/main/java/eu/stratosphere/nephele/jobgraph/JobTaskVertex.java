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
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.template.IllegalConfigurationException;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * A JobTaskVertex is the vertex type for regular tasks (with both input and output) in Nephele.
 * Tasks running inside a JobTaskVertex must specify at least one record reader and one record writer.
 * 
 * @author warneke
 */
public class JobTaskVertex extends AbstractJobVertex {

	/**
	 * The task attached to this vertex.
	 */
	private Class<? extends AbstractTask> taskClass = null;

	/**
	 * Creates a new job task vertex with the specified name.
	 * 
	 * @param name
	 *        the name for the new job task vertex
	 * @param id
	 *        the ID of this vertex
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	public JobTaskVertex(String name, JobVertexID id, JobGraph jobGraph) {
		super(name, id, jobGraph);

		jobGraph.addVertex(this);
	}

	/**
	 * Creates a new job task vertex with the specified name.
	 * 
	 * @param name
	 *        the name for the new job task vertex
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	public JobTaskVertex(String name, JobGraph jobGraph) {
		super(name, null, jobGraph);

		jobGraph.addVertex(this);
	}

	/**
	 * Creates a new job task vertex.
	 * 
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	public JobTaskVertex(JobGraph jobGraph) {
		super(null, null, jobGraph);

		jobGraph.addVertex(this);
	}

	/**
	 * Sets the class of the vertex's task.
	 * 
	 * @param taskClass
	 *        the class of the vertex's task
	 */
	public void setTaskClass(Class<? extends AbstractTask> taskClass) {
		this.taskClass = taskClass;
	}

	/**
	 * Returns the class of the vertex's task.
	 * 
	 * @return the class of the vertex's task or <code>null</code> if the class has not yet been set
	 */
	public Class<? extends AbstractTask> getTaskClass() {
		return this.taskClass;
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);

		final boolean isNotNull = in.readBoolean();
		if (!isNotNull) {
			return;
		}

		// Read the name of the class and try to instantiate the class object

		final ClassLoader cl = LibraryCacheManager.getClassLoader(this.getJobGraph().getJobID());
		if (cl == null) {
			throw new IOException("Cannot find class loader for vertex " + getID());
		}

		// Read the name of the expected class
		final String className = StringRecord.readString(in);

		try {
			this.taskClass = (Class<? extends AbstractTask>) Class.forName(className, true, cl);
		} catch (ClassNotFoundException cnfe) {
			throw new IOException("Class " + className + " not found in one of the supplied jar files: "
				+ StringUtils.stringifyException(cnfe));
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		if (this.taskClass == null) {
			out.writeBoolean(false);
			return;
		}

		out.writeBoolean(true);

		// Write out the name of the class
		StringRecord.writeString(out, this.taskClass.getName());

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void checkConfiguration(AbstractInvokable invokable) throws IllegalConfigurationException {

		// Delegate check to invokable
		invokable.checkConfiguration();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumNumberOfSubtasks(AbstractInvokable invokable) {

		// Delegate call to invokable
		return invokable.getMaximumNumberOfSubtasks();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMinimumNumberOfSubtasks(AbstractInvokable invokable) {

		// Delegate call to invokable
		return invokable.getMinimumNumberOfSubtasks();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Class<? extends AbstractInvokable> getInvokableClass() {

		return this.taskClass;
	}
}
