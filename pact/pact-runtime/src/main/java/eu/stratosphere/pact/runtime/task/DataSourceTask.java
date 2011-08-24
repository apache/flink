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
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.pact.common.io.input.InputFormat;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * DataSourceTask which is executed by a Nephele task manager.
 * The task reads data and uses an InputFormat to create records from the input.
 * 
 * @see eu.stratosphere.pact.common.io.input.InputFormat
 * 
 * @author Moritz Kaufmann
 * @author Fabian Hueske
 * @author Stephan Ewen
 */

@SuppressWarnings({ "unchecked", "rawtypes" })
public class DataSourceTask extends AbstractInputTask<InputSplit>
{
	// Obtain DataSourceTask Logger
	private static final Log LOG = LogFactory.getLog(DataSourceTask.class);

	// Output collector
	private OutputCollector<Key, Value> output;

	// InputFormat instance
	private InputFormat<InputSplit, Key, Value> format;

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
		initOutputCollector();

		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("Finished registering input and output"));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception
	{	
		KeyValuePair<Key, Value> pair = null;

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
			
			final InputFormat format = this.format;
			
			try {
				// open input format
				format.open(split);
	
				if (LOG.isDebugEnabled())
					LOG.debug(getLogString("Starting to read input from split " + split.toString()));
	
				// as long as there is data to read
				while (!this.taskCanceled && !format.reachedEnd()) {
					pair = format.createPair();
					// build next pair and ship pair if it is valid
					if (format.nextRecord(pair)) {
						this.output.collect(pair.getKey(), pair.getValue());
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
		this.config = new TaskConfig(getRuntimeConfiguration());

		// obtain stub implementation class
		try {
			ClassLoader cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			Class<? extends InputFormat> formatClass = this.config.getStubClass(InputFormat.class, cl);
			// obtain instance of stub implementation
			this.format = formatClass.newInstance();
			// configure stub implementation
		}
		catch (IOException ioe) {
			throw new RuntimeException("Library cache manager could not be instantiated.", ioe);
		}
		catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("InputFormat implementation class was not found.", cnfe);
		}
		catch (InstantiationException ie) {
			throw new RuntimeException("InputFormat implementation could not be instanciated. " +
					"Likely reasons are either a missing nullary constructor, or that the class is abstract.", ie);
		}
		catch (IllegalAccessException iae) {
			throw new RuntimeException("InputFormat implementation class or its nullary constructor are " +
					"not accessible (private or protected).", iae);
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
	 * Creates a writer for each output. Creates an OutputCollector which
	 * forwards its input to all writers.
	 */
	private void initOutputCollector()
	{
		boolean fwdCopyFlag = false;

		// create output collector
		this.output = new OutputCollector<Key, Value>();

		// create a writer for each output
		for (int i = 0; i < config.getNumOutputs(); i++) {
			// obtain OutputEmitter from output ship strategy
			OutputEmitter oe = new OutputEmitter(config.getOutputShipStrategy(i));
			// create writer
			RecordWriter<KeyValuePair<Key, Value>> writer;
			writer = new RecordWriter<KeyValuePair<Key, Value>>(this,
				(Class<KeyValuePair<Key, Value>>) (Class<?>) KeyValuePair.class, oe);

			// add writer to output collector
			// the first writer does not need to send a copy
			// all following must send copies
			// TODO smarter decision are possible here, e.g. decide which channel may not need to copy, ...
			output.addWriter(writer, fwdCopyFlag);
			fwdCopyFlag = true;
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
