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
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.generic.io.InputFormat;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.runtime.plugable.PactRecordSerializer;
import eu.stratosphere.pact.runtime.shipping.OutputCollector;
import eu.stratosphere.pact.runtime.shipping.PactRecordOutputCollector;
import eu.stratosphere.pact.runtime.task.chaining.ChainedDriver;
import eu.stratosphere.pact.runtime.task.chaining.ChainedMapDriver;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * DataSourceTask which is executed by a Nephele task manager. The task reads data and uses an 
 * {@link InputFormat} to create records from the input.
 * 
 * @see eu.stratosphere.pact.generic.io.InputFormat
 * 
 * @author Stephan Ewen
 * @author Fabian Hueske
 * @author Moritz Kaufmann
 */

public class DataSourceTask<OT> extends AbstractInputTask<InputSplit>
{
	// Obtain DataSourceTask Logger
	private static final Log LOG = LogFactory.getLog(DataSourceTask.class);

	// Output collector
	private Collector<OT> output;

	// InputFormat instance
	private InputFormat<OT, InputSplit> format;

	// type serializer for the input
	private TypeSerializer<OT> serializer;
	
	// Task configuration
	private TaskConfig config;
	
	// tasks chained to this data source
	private ArrayList<ChainedDriver<?, ?>> chainedTasks;
	
	private ClassLoader userCodeClassLoader;

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

		if (this.userCodeClassLoader == null) {
			try {
				this.userCodeClassLoader = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			}
			catch (IOException ioe) {
				throw new RuntimeException("Usercode ClassLoader could not be obtained for job: " + 
							getEnvironment().getJobID(), ioe);
			}
		}
		
		initInputFormat(this.userCodeClassLoader);
		
		try {
			initOutputs(this.userCodeClassLoader);
		} catch (Exception ex) {
			throw new RuntimeException("The initialization of the DataSource's outputs caused an error: " + 
				ex.getMessage(), ex);
		}

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
			RegularPactTask.openChainedTasks(this.chainedTasks, this);
			
			// get input splits to read
			final Iterator<InputSplit> splitIterator = getInputSplits();
			final OT record = this.serializer.createInstance();
	
			// for each assigned input split
			while (!this.taskCanceled && splitIterator.hasNext())
			{
				// get start and end
				final InputSplit split = splitIterator.next();
	
				if (LOG.isDebugEnabled())
					LOG.debug(getLogString("Opening input split " + split.toString()));
				
				final InputFormat<OT, InputSplit> format = this.format;
			
				// open input format
				format.open(split);
	
				if (LOG.isDebugEnabled())
					LOG.debug(getLogString("Starting to read input from split " + split.toString()));
				
				try {
					// ======= special-case the PactRecord, to help the JIT and avoid some casts ======
					if (record.getClass() == PactRecord.class) {
						final PactRecord pactRecord = (PactRecord) record;
						@SuppressWarnings("unchecked")
						final InputFormat<PactRecord, InputSplit> inFormat = (InputFormat<PactRecord, InputSplit>) format;
						
						if (this.output instanceof PactRecordOutputCollector)
						{
							// PactRecord going directly into network channels
							final PactRecordOutputCollector output = (PactRecordOutputCollector) this.output;
							while (!this.taskCanceled && !inFormat.reachedEnd()) {
								// build next pair and ship pair if it is valid
								pactRecord.clear();
								if (inFormat.nextRecord(pactRecord)) {
									output.collect(pactRecord);
								}
							}
						} else if (this.output instanceof ChainedMapDriver) {
							// PactRecord going to a chained map task
							@SuppressWarnings("unchecked")
							final ChainedMapDriver<PactRecord, ?> output = (ChainedMapDriver<PactRecord, ?>) this.output;
							
							// as long as there is data to read
							while (!this.taskCanceled && !inFormat.reachedEnd()) {
								// build next pair and ship pair if it is valid
								pactRecord.clear();
								if (inFormat.nextRecord(pactRecord)) {
									output.collect(pactRecord);
								}
							}
						} else {
							// PactRecord going to some other chained task
							@SuppressWarnings("unchecked")
							final Collector<PactRecord> output = (Collector<PactRecord>) this.output;
							// as long as there is data to read
							while (!this.taskCanceled && !inFormat.reachedEnd()) {
								// build next pair and ship pair if it is valid
								pactRecord.clear();
								if (inFormat.nextRecord(pactRecord)) {
									output.collect(pactRecord);
								}
							}
						}
					} else {
						// general types. we make a case distinction here for the common cases, in order to help
						// JIT method inlining
						if (this.output instanceof OutputCollector)
						{
							final OutputCollector<OT> output = (OutputCollector<OT>) this.output;
							
							// as long as there is data to read
							while (!this.taskCanceled && !format.reachedEnd()) {
								// build next pair and ship pair if it is valid
								if (format.nextRecord(record)) {
									output.collect(record);
								}
							}
						}
						else if (this.output instanceof ChainedMapDriver)
						{
							@SuppressWarnings("unchecked")
							final ChainedMapDriver<OT, ?> output = (ChainedMapDriver<OT, ?>) this.output;
							
							// as long as there is data to read
							while (!this.taskCanceled && !format.reachedEnd()) {
								// build next pair and ship pair if it is valid
								if (format.nextRecord(record)) {
									output.collect(record);
								}
							}
						}
						else {
							final Collector<OT> output = this.output;
							
							// as long as there is data to read
							while (!this.taskCanceled && !format.reachedEnd()) {
								// build next pair and ship pair if it is valid
								if (format.nextRecord(record)) {
									output.collect(record);
								}
							}
						}
					}
					
					if (LOG.isDebugEnabled() && !this.taskCanceled) {
						LOG.debug(getLogString("Closing input split " + split.toString()));
					}
				} finally {
					// close. We close here such that a regular close throwing an exception marks a task as failed.
					format.close();
				}
			} // end for all input splits
			
			// close the collector. if it is a chaining task collector, it will close its chained tasks
			this.output.close();
			
			// close all chained tasks letting them report failure
			RegularPactTask.closeChainedTasks(this.chainedTasks, this);
		}
		catch (Exception ex) {
			// close the input, but do not report any exceptions, since we already have another root cause
			try {
				this.format.close();
			} catch (Throwable t) {}
			
			RegularPactTask.cancelChainedTasks(this.chainedTasks);
			
			// drop exception, if the task was canceled
			if (!this.taskCanceled) {
				RegularPactTask.logAndThrowException(ex, this);
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
	 * Sets the class-loader to be used to load the user code.
	 * 
	 * @param cl The class-loader to be used to load the user code.
	 */
	public void setUserCodeClassLoader(ClassLoader cl)
	{
		this.userCodeClassLoader = cl;
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

		// instantiate the stub
		@SuppressWarnings("unchecked")
		Class<InputFormat<OT, InputSplit>> superClass = (Class<InputFormat<OT, InputSplit>>) (Class<?>) InputFormat.class;
		this.format = RegularPactTask.instantiateUserCode(this.config, cl, superClass);
		
		// get the factory for the type serializer
		try {
			final Class<? extends TypeSerializerFactory<OT>> serializerFactoryClass = this.config.getSerializerFactoryForOutput(cl);
			if (serializerFactoryClass == null) {
				@SuppressWarnings("unchecked")
				TypeSerializer<OT> ps = (TypeSerializer<OT>) PactRecordSerializer.get();
				this.serializer = ps;
			} else {
				TypeSerializerFactory<OT> serializerFactory = InstantiationUtil.instantiate(serializerFactoryClass, TypeSerializerFactory.class);
				this.serializer = serializerFactory.getSerializer();
			}
			
		} catch (ClassNotFoundException cnfex) {
			throw new RuntimeException("The class registered as output serializer factory could not be loaded.", cnfex);
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
	private void initOutputs(ClassLoader cl) throws Exception
	{
		this.chainedTasks = new ArrayList<ChainedDriver<?, ?>>();
		this.output = RegularPactTask.initOutputs(this, cl, this.config, this.chainedTasks, null);
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
	@SuppressWarnings("unchecked")
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
		return RegularPactTask.constructLogString(message, taskName, this);
	}
}