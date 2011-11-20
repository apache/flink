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
package eu.stratosphere.pact.runtime.task;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.BipartiteDistributionPattern;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.PointwiseDistributionPattern;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.task.util.NepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

/**
 * The abstract base class for all Pact tasks. Encapsulated common behavior and implements the main life-cycle
 * of the user code.
 *
 * @author Stephan Ewen
 * @author Fabian Hueske
 */
public abstract class AbstractPactTask<T extends Stub> extends AbstractTask
{
	protected static final Log LOG = LogFactory.getLog(AbstractPactTask.class);
	
	protected TaskConfig config;
	
	protected T stub;
	
	protected MutableObjectIterator<PactRecord>[] inputs;
	
	protected OutputCollector output;
	
	protected ClassLoader userCodeClassLoader;
	
	protected volatile boolean running;
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the number of inputs (= Nephele Gates and Readers) that the task has.
	 * 
	 * @return The number of inputs.
	 */
	public abstract int getNumberOfInputs();
	
	/**
	 * Gets the class of the stub type that is run by this task. For example, a <tt>MapTask</tt> should return
	 * <code>MapStub.class</code>.   
	 * 
	 * @return The class of the stub type run by the task.
	 */
	public abstract Class<T> getStubType();
	
	/**
	 * This method is called before the user code is opened. An exception thrown by this method
	 * signals failure of the task.
	 * 
	 * @throws Exception Exceptions may be forwarded and signal task failure.
	 */
	public abstract void prepare() throws Exception;
	
	/**
	 * The main operation method of the task. It should call the user code with the data subsets until
	 * the input is depleted.
	 * 
	 * @throws Exception Any exception thrown by this method signals task failure. Because exceptions in the user
	 *                   code typically signal situations where this instance in unable to procede, exceptions
	 *                   from the user code should be forwarded.
	 */
	public abstract void run()
		throws Exception; 
	
	/**
	 * This method is invoked in any case (clean termination and exception) at the end of the tasks operation.
	 * 
	 * @throws Exception Exceptions may be forwarded.
	 */
	public abstract void cleanup() throws Exception;
	
	// ------------------------------------------------------------------------
	//                       Nephele Task Interface
	// ------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInvokable#registerInputOutput()
	 */
	@Override
	public void registerInputOutput()
	{
		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("Start registering input and output."));

		initConfigAndStub(getStubType());
		initInputs();
		initOutputs();

		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("Finished registering input and output."));
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInvokable#invoke()
	 */
	@Override
	public void invoke() throws Exception
	{
		if (LOG.isInfoEnabled())
			LOG.info(getLogString("Start PACT code."));
		
		boolean stubOpen = false;
		this.running = true;
		
		try {
			// run the data preparation
			try {
				prepare();
			}
			catch (Throwable t) {
				// if the preparation caused an error, clean up
				// errors during clean-up are swallowed, because we have already a root exception
				throw new Exception("The data preparation for task '" + this.getEnvironment().getTaskName() + 
					"' , caused an error: " + t.getMessage(), t);
			}
			
			// open stub implementation
			try {
				this.stub.open(this.config.getStubParameters());
				stubOpen = true;
			}
			catch (Throwable t) {
				throw new Exception("The user defined 'open()' method caused an exception: " + t.getMessage(), t);
			}
			
			// run the user code
			run();
			
			// close. We close here such that a regular close throwing an exception marks a task as failed.
			if (this.running) {
				this.stub.close();
				stubOpen = false;
			}
		}
		catch (Exception ex) {
			// close the input, but do not report any exceptions, since we already have another root cause
			if (stubOpen) {
				try {
					this.stub.close();
				}
				catch (Throwable t) {}
			}
			
			// drop exception, if task was canceled, because we already have a root exception.
			if (this.running) {
				if (LOG.isErrorEnabled())
					LOG.error(getLogString("Unexpected ERROR in PACT code."), ex);
				throw ex;
			}
		}
		finally {
			// close the output collector and make sure we run the cleanup method
			try {
				this.output.close();
			}
			catch (Throwable t) {}
			
			cleanup();
		}

		if (this.running) {
			if (LOG.isInfoEnabled())
				LOG.info(getLogString("Finished PACT code."));
		}
		else {
			if (LOG.isWarnEnabled())
				LOG.warn(getLogString("PACT code cancelled."));
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInvokable#cancel()
	 */
	@Override
	public void cancel() throws Exception
	{
		this.running = false;
		if (LOG.isWarnEnabled())
			LOG.warn(getLogString("Cancelling PACT code"));
	}
	
	// ------------------------------------------------------------------------
	//                       Task Setup and Teardown
	// ------------------------------------------------------------------------
	
	/**
	 * Initializes the Stub class implementation and configuration.
	 * 
	 * @throws RuntimeException Thrown, if the stub class could not be loaded, instantiated,
	 *                          or caused an exception while being configured.
	 */
	protected void initConfigAndStub(Class<? super T> stubSuperClass)
	{
		// obtain task configuration (including stub parameters)
		this.config = new TaskConfig(getRuntimeConfiguration());

		// obtain stub implementation class
		try {
			this.userCodeClassLoader = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			@SuppressWarnings("unchecked")
			Class<T> stubClass = (Class<T>) this.config.getStubClass(stubSuperClass, this.userCodeClassLoader);
			
			this.stub = InstantiationUtil.instantiate(stubClass, stubSuperClass);
		}
		catch (IOException ioe) {
			throw new RuntimeException("Library cache manager could not be instantiated.", ioe);
		}
		catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("The stub implementation class was not found.", cnfe);
		}
		catch (ClassCastException ccex) {
			throw new RuntimeException("The stub class is not a proper subclass of " + stubSuperClass.getName(), ccex); 
		}
	}
	
	/**
	 * Creates the record readers for the number of inputs as defined by {@link #getNumberOfInputs()}.
	 */
	protected void initInputs()
	{
		int numInputs = getNumberOfInputs();
		
		@SuppressWarnings("unchecked")
		final MutableObjectIterator<PactRecord>[] inputs = new MutableObjectIterator[numInputs];
		
		for (int i = 0; i < numInputs; i++)
		{
			final ShipStrategy shipStrategy = config.getInputShipStrategy(i);
			DistributionPattern dp = null;
			
			switch (shipStrategy)
			{
			case FORWARD:
			case PARTITION_LOCAL_HASH:
			case PARTITION_LOCAL_RANGE:
				dp = new PointwiseDistributionPattern();
				break;
			case PARTITION_HASH:
			case PARTITION_RANGE:
			case BROADCAST:
			case SFR:
				dp = new BipartiteDistributionPattern();
				break;
			default:
				throw new RuntimeException("Invalid input ship strategy provided for input " + 
					i + ": " + shipStrategy.name());
			}
			
			inputs[i] = new NepheleReaderIterator(new MutableRecordReader<PactRecord>(this, dp));
		}
		this.inputs = inputs;
	}
	
	/**
	 * Creates a writer for each output. Creates an OutputCollector which forwards its input to all writers.
	 * The output collector applies the configured shipping strategy.
	 */
	protected void initOutputs()
	{
		final int numOutputs = config.getNumOutputs();
		
		// create output collector
		this.output = new OutputCollector();
		
		final JobID jobId = getEnvironment().getJobID();
		final ClassLoader cl;
		try {
			cl = LibraryCacheManager.getClassLoader(jobId);
		}
		catch (IOException ioe) {
			throw new RuntimeException("Library cache manager could not be instantiated.", ioe);
		}
		
		// create a writer for each output
		for (int i = 0; i < numOutputs; i++)
		{
			// create the OutputEmitter from output ship strategy
			final ShipStrategy strategy = config.getOutputShipStrategy(i);
			final int[] keyPositions = this.config.getOutputShipKeyPositions(i);
			final Class<? extends Key>[] keyClasses;
			try {
				keyClasses= this.config.getOutputShipKeyTypes(i, cl);
			}
			catch (ClassNotFoundException cnfex) {
				throw new RuntimeException("The classes for the keys after which output " + i + 
					" ships the records could not be loaded.");
			}
			
			OutputEmitter oe = (keyPositions == null || keyClasses == null) ?
					new OutputEmitter(strategy) :
					new OutputEmitter(strategy, keyPositions, keyClasses);
					
			// create writer
			RecordWriter<PactRecord> writer= new RecordWriter<PactRecord>(this, PactRecord.class, oe);

			// add writer to output collector
			output.addWriter(writer);
		}
	}
	
	// ------------------------------------------------------------------------
	//                               Utilities
	// ------------------------------------------------------------------------
	
	/**
	 * Utility function that composes a string for logging purposes. The string includes the given message and
	 * the index of the task in its task group together with the number of tasks in the task group.
	 *  
	 * @param message The main message for the log.
	 * @return The string ready for logging.
	 */
	protected String getLogString(String message)
	{
		StringBuilder bld = new StringBuilder(128);	
		bld.append(message);
		bld.append(':').append(' ');
		bld.append(this.getEnvironment().getTaskName());
		bld.append(' ').append('(');
		bld.append(this.getEnvironment().getIndexInSubtaskGroup() + 1);
		bld.append('/');
		bld.append(this.getEnvironment().getCurrentNumberOfSubtasks());
		bld.append(')');
		return bld.toString();
	}
}
