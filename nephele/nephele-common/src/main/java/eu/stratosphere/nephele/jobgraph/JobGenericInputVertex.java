package eu.stratosphere.nephele.jobgraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.IllegalConfigurationException;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;


/**
 * 
 */
public class JobGenericInputVertex extends JobInputVertex
{
	/**
	 * Class of input task.
	 */
	protected Class<? extends AbstractInputTask<?>> inputClass = null;

	/**
	 * Creates a new job input vertex with the specified name.
	 * 
	 * @param name The name of the new job file input vertex.
	 * @param id The ID of this vertex.
	 * @param jobGraph The job graph this vertex belongs to.
	 */
	public JobGenericInputVertex(String name, JobVertexID id, JobGraph jobGraph) {
		super(name, id, jobGraph);
	}

	/**
	 * Creates a new job file input vertex with the specified name.
	 * 
	 * @param name The name of the new job file input vertex.
	 * @param jobGraph The job graph this vertex belongs to.
	 */
	public JobGenericInputVertex(String name, JobGraph jobGraph) {
		super(name, null, jobGraph);
	}

	/**
	 * Creates a new job file input vertex.
	 * 
	 * @param jobGraph The job graph this vertex belongs to.
	 */
	public JobGenericInputVertex(JobGraph jobGraph) {
		super(null, null, jobGraph);
	}

	/**
	 * Sets the class of the vertex's input task.
	 * 
	 * @param inputClass The class of the vertex's input task.
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
	public void read(DataInput in) throws IOException
	{
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
				this.inputClass = (Class<? extends AbstractInputTask<?>>) Class.forName(className, true, cl).asSubclass(AbstractInputTask.class);
			}
			catch (ClassNotFoundException cnfe) {
				throw new IOException("Class " + className + " not found in one of the supplied jar files: "
					+ StringUtils.stringifyException(cnfe));
			}
			catch (ClassCastException ccex) {
				throw new IOException("Class " + className + " is not a subclass of "
					+ AbstractInputTask.class.getName() + ": " + StringUtils.stringifyException(ccex));
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException
	{
		super.write(out);

		// Write out the name of the class
		if (this.inputClass == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			StringRecord.writeString(out, this.inputClass.getName());
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
		}
		catch (IllegalConfigurationException icex) {
			throw icex; // simply forward
		}
		catch (Throwable t) {
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
