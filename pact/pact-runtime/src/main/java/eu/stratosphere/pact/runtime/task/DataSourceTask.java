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
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.pact.common.io.InputFormat;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

/**
 * DataSourceTask which is executed by a Nephele task manager. The task reads data and uses an 
 * {@link InputFormat} to create records from the input.
 * 
 * @see eu.stratosphere.pact.common.io.InputFormat
 * 
 * @author Stephan Ewen
 * @author Fabian Hueske
 * @author Moritz Kaufmann
 */

public class DataSourceTask extends AbstractInputTask<InputSplit>
{
	// Obtain DataSourceTask Logger
	private static final Log LOG = LogFactory.getLog(DataSourceTask.class);

	// Output collector
	private OutputCollector output;

	// InputFormat instance
	private InputFormat<InputSplit> format;

	// Task configuration
	private TaskConfig config;

	// cancel flag
	private volatile boolean taskCanceled = false;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput()
	{
		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("Start registering input and output"));

		// Initialize InputFormat
		initInputFormat();

		// Initialize OutputCollector
		initOutputs();

		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("Finished registering input and output"));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception
	{	
		if (LOG.isInfoEnabled())
			LOG.info(getLogString("Start PACT code"));

		// get file splits to read
		final Iterator<InputSplit> splitIterator = getInputSplits();

		// for each assigned input split
		while (!this.taskCanceled && splitIterator.hasNext())
		{
			// get start and end
			final InputSplit split = splitIterator.next();

			if (LOG.isDebugEnabled())
				LOG.debug(getLogString("Opening input split " + split.toString()));

			if (this.taskCanceled) {
				return;
			}
			
			final InputFormat<InputSplit> format = this.format;
			
			try {
				// open input format
				format.open(split);
	
				if (LOG.isDebugEnabled())
					LOG.debug(getLogString("Starting to read input from split " + split.toString()));
	
				final PactRecord record = new PactRecord();
				
				// as long as there is data to read
				while (!this.taskCanceled && !format.reachedEnd())
				{
					// build next pair and ship pair if it is valid
					if (format.nextRecord(record)) {
						this.output.collect(record);
					}
				}

				// close. We close here such that a regular close throwing an exception marks a task as failed.
				if (!this.taskCanceled) {
					if (LOG.isDebugEnabled())
						LOG.debug(getLogString("Closing input split " + split.toString()));
					
					format.close();
				}
			}
			catch (Exception ex) {
				// close the input, but do not report any exceptions, since we already have another root cause
				try {
					format.close();
				}
				catch (Throwable t) {}
				
				// drop exception, if the task was canceled
				if (!this.taskCanceled) {
					if (LOG.isErrorEnabled())
						LOG.error(getLogString("Unexpected ERROR in PACT code"));
					throw ex;
				}	
			}
		}

		if (!this.taskCanceled) {
			if (LOG.isInfoEnabled())
				LOG.info(getLogString("Finished PACT code"));
		}
		else {
			if (LOG.isWarnEnabled())
				LOG.warn(getLogString("PACT code cancelled"));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInvokable#cancel()
	 */
	@Override
	public void cancel() throws Exception
	{
		this.taskCanceled = true;
		if (LOG.isWarnEnabled())
			LOG.warn(getLogString("Cancelling PACT code"));
	}

	/**
	 * Initializes the InputFormat implementation and configuration.
	 * 
	 * @throws RuntimeException
	 *         Throws if instance of InputFormat implementation can not be
	 *         obtained.
	 */
	private void initInputFormat()
	{
		// obtain task configuration (including stub parameters)
		this.config = new TaskConfig(getTaskConfiguration());

		// obtain stub implementation class
		try {
			ClassLoader cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			@SuppressWarnings("unchecked")
			Class<? extends InputFormat<InputSplit>> formatClass = (Class<? extends InputFormat<InputSplit>>) this.config.getStubClass(InputFormat.class, cl);
			
			this.format = InstantiationUtil.instantiate(formatClass, InputFormat.class);
		}
		catch (IOException ioe) {
			throw new RuntimeException("Library cache manager could not be instantiated.", ioe);
		}
		catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("InputFormat implementation class was not found.", cnfe);
		}
		catch (ClassCastException ccex) {
			throw new RuntimeException("Format format class is not a proper subclass of " + InputFormat.class.getName(), ccex); 
		}
		
		// configure the stub. catch exceptions here extra, to report them as originating from the user code 
		try {
			this.format.configure(this.config.getStubParameters());
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
	//                              Input Split creation
	// ------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInputTask#computeInputSplits(int)
	 */
	@Override
	public InputSplit[] computeInputSplits(int requestedMinNumber) throws Exception
	{
		// we have to be sure that the format is instantiated at this point
		if (this.format == null) {
			throw new IllegalStateException("BUG: Input format hast not been instantiated, yet.");
		}
		
		return this.format.createInputSplits(requestedMinNumber);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInputTask#getInputSplitType()
	 */
	@Override
	public Class<InputSplit> getInputSplitType()
	{
		// we have to be sure that the format is instantiated at this point
		if (this.format == null) {
			throw new IllegalStateException("BUG: Input format hast not been instantiated, yet.");
		}
		
		return this.format.getInputSplitType();
	}
	
	// ------------------------------------------------------------------------
	//                             
	// ------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInvokable#getMinimumNumberOfSubtasks()
	 */
	@Override
	public int getMinimumNumberOfSubtasks()
	{
		return 1;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInvokable#getMaximumNumberOfSubtasks()
	 */
	@Override
	public int getMaximumNumberOfSubtasks()
	{
		// since splits can in theory be arbitrarily small, we report a possible infinite number of subtasks.
		return -1;
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
	private String getLogString(String message)
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
