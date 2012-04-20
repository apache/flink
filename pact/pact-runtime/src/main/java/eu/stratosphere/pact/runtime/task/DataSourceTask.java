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
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.pact.common.io.InputFormat;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.task.chaining.ChainedTask;
import eu.stratosphere.pact.runtime.task.chaining.ChainedMapTask;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

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
	private Collector output;

	// InputFormat instance
	private InputFormat<InputSplit> format;

	// Task configuration
	private TaskConfig config;
	
	private ArrayList<ChainedTask> chainedTasks;

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

		ClassLoader cl;
		try {
			cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
		}
		catch (IOException ioe) {
			throw new RuntimeException("Usercode ClassLoader could not be obtained for job: " + 
						getEnvironment().getJobID(), ioe);
		}
		
		initInputFormat(cl);
		initOutputs(cl);

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
		
		try {
			// start all chained tasks
			AbstractPactTask.openChainedTasks(this.chainedTasks, this);
			
			// get input splits to read
			final Iterator<InputSplit> splitIterator = getInputSplits();
	
			// for each assigned input split
			while (!this.taskCanceled && splitIterator.hasNext())
			{
				// get start and end
				final InputSplit split = splitIterator.next();
	
				if (LOG.isDebugEnabled())
					LOG.debug(getLogString("Opening input split " + split.toString()));
				
				final InputFormat<InputSplit> format = this.format;
			
				// open input format
				format.open(split);
	
				if (LOG.isDebugEnabled())
					LOG.debug(getLogString("Starting to read input from split " + split.toString()));
	
				final PactRecord record = new PactRecord();
				
				// we make a case distinction here for the common cases, in order to help
				// JIT method inlining
				if (this.output instanceof OutputCollector)
				{
					final OutputCollector output = (OutputCollector) this.output;
					
					// as long as there is data to read
					while (!this.taskCanceled && !format.reachedEnd()) {
						// build next pair and ship pair if it is valid
						if (format.nextRecord(record)) {
							output.collect(record);
						}
					}
				}
				else if (this.output instanceof ChainedMapTask)
				{
					final ChainedMapTask output = (ChainedMapTask) this.output;
					
					// as long as there is data to read
					while (!this.taskCanceled && !format.reachedEnd()) {
						// build next pair and ship pair if it is valid
						if (format.nextRecord(record)) {
							output.collect(record);
						}
					}
				}
				else {
					final Collector output = this.output;
					
					// as long as there is data to read
					while (!this.taskCanceled && !format.reachedEnd()) {
						// build next pair and ship pair if it is valid
						if (format.nextRecord(record)) {
							output.collect(record);
						}
					}
				}

				// close. We close here such that a regular close throwing an exception marks a task as failed.
				if (!this.taskCanceled) {
					if (LOG.isDebugEnabled())
						LOG.debug(getLogString("Closing input split " + split.toString()));
					
					format.close();
				}
			} // end for all input splits
			
			// close the collector. if it is a chaining task collector, it will close its chained tasks
			this.output.close();
			
			// close all chained tasks letting them report failure
			AbstractPactTask.closeChainedTasks(this.chainedTasks, this);
		}
		catch (Exception ex) {
			// close the input, but do not report any exceptions, since we already have another root cause
			try {
				this.format.close();
			} catch (Throwable t) {}
			
			AbstractPactTask.cancelChainedTasks(this.chainedTasks);
			
			// drop exception, if the task was canceled
			if (!this.taskCanceled) {
				AbstractPactTask.logAndThrowException(ex, this);
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
	private void initInputFormat(ClassLoader cl)
	{
		// obtain task configuration (including stub parameters)
		this.config = new TaskConfig(getTaskConfiguration());

		@SuppressWarnings("unchecked")
		Class<InputFormat<InputSplit>> superClass = (Class<InputFormat<InputSplit>>) (Class<?>) InputFormat.class;
		this.format = AbstractPactTask.instantiateUserCode(this.config, cl, superClass);
		
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
	private void initOutputs(ClassLoader cl)
	{
		this.chainedTasks = new ArrayList<ChainedTask>();
		this.output = AbstractPactTask.initOutputs(this, cl, this.config, this.chainedTasks);
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
		
		return (Class<InputSplit>) this.format.getInputSplitType();
	}
	
	// ------------------------------------------------------------------------
	//                       Control of Parallelism
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
		return getLogString(message, this.getEnvironment().getTaskName());
	}
	
	/**
	 * Utility function that composes a string for logging purposes. The string includes the given message and
	 * the index of the task in its task group together with the number of tasks in the task group.
	 *  
	 * @param message The main message for the log.
	 * @param taskName The name of the task.
	 * @return The string ready for logging.
	 */
	private String getLogString(String message, String taskName)
	{
		return AbstractPactTask.constructLogString(message, taskName, this);
	}
}