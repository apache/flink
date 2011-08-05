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

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class AbstractPactTask<T extends Stub> extends AbstractTask
{
	protected TaskConfig config;
	
	protected T stub;
	
	protected OutputCollector output;
	
	
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
			ClassLoader cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			@SuppressWarnings("unchecked")
			Class<T> stubClass = (Class<T>) this.config.getStubClass(stubSuperClass, cl);
			
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

	protected void openStub()
	{
		// configure the stub. catch exceptions here extra, to report them as originating from the user code 
		try {
			this.stub.open(this.config.getStubParameters());
		}
		catch (Throwable t) {
			throw new RuntimeException("The user defined 'configure()' method caused an error: " + t.getMessage(), t);
		}
	}
	
	/**
	 * Creates a writer for each output. Creates an OutputCollector which forwards its input to all writers.
	 * The output collector applies the configured shipping strategy.
	 */
	protected void initOutputs()
	{
		final int numOutputs = config.getNumOutputs();
		
		// create output collector
		boolean fwdCopyFlag = false;
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
					new OutputEmitter(strategy, jobId, keyPositions, keyClasses);
					
			// create writer
			RecordWriter<PactRecord> writer= new RecordWriter<PactRecord>(this, PactRecord.class, oe);

			// add writer to output collector
			output.addWriter(writer, fwdCopyFlag);
			fwdCopyFlag = true;
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
