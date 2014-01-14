/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.runtime.task.chaining.ExceptionInChainedStubException;
import eu.stratosphere.runtime.io.api.BufferWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.accumulators.Accumulator;
import eu.stratosphere.api.common.io.InputFormat;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.io.InputSplit;
import eu.stratosphere.nephele.execution.CancelTaskException;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.pact.runtime.shipping.OutputCollector;
import eu.stratosphere.pact.runtime.shipping.RecordOutputCollector;
import eu.stratosphere.pact.runtime.task.chaining.ChainedCollectorMapDriver;
import eu.stratosphere.pact.runtime.task.chaining.ChainedDriver;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

/**
 * DataSourceTask which is executed by a Nephele task manager. The task reads data and uses an 
 * {@link InputFormat} to create records from the input.
 * 
 * @see eu.stratosphere.api.common.io.InputFormat
 */
public class DataSourceTask<OT> extends AbstractInputTask<InputSplit> {
	
	// Obtain DataSourceTask Logger
	private static final Log LOG = LogFactory.getLog(DataSourceTask.class);

	private List<BufferWriter> eventualOutputs;

	// Output collector
	private Collector<OT> output;

	// InputFormat instance
	private InputFormat<OT, InputSplit> format;

	// type serializer for the input
	private TypeSerializerFactory<OT> serializerFactory;
	
	// Task configuration
	private TaskConfig config;
	
	// tasks chained to this data source
	private ArrayList<ChainedDriver<?, ?>> chainedTasks;
	
	private ClassLoader userCodeClassLoader;

	// cancel flag
	private volatile boolean taskCanceled = false;


	@Override
	public void registerInputOutput()
	{
		if (LOG.isDebugEnabled()) {
			LOG.debug(getLogString("Start registering input and output"));
		}

		if (this.userCodeClassLoader == null) {
			try {
				this.userCodeClassLoader = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			}
			catch (IOException ioe) {
				throw new RuntimeException("Usercode ClassLoader could not be obtained for job: " + 
							getEnvironment().getJobID(), ioe);
			}
		}
		
		// obtain task configuration (including stub parameters)
		Configuration taskConf = getTaskConfiguration();
		taskConf.setClassLoader(this.userCodeClassLoader);
		this.config = new TaskConfig(taskConf);
		
		initInputFormat(this.userCodeClassLoader);
		
		try {
			initOutputs(this.userCodeClassLoader);
		} catch (Exception ex) {
			throw new RuntimeException("The initialization of the DataSource's outputs caused an error: " + 
				ex.getMessage(), ex);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug(getLogString("Finished registering input and output"));
		}
	}


	@Override
	public void invoke() throws Exception {
		
		if (LOG.isDebugEnabled()) {
			LOG.debug(getLogString("Starting data source operator"));
		}
		
		final TypeSerializer<OT> serializer = this.serializerFactory.getSerializer();
		
		try {
			// initialize the serializers (one per channel) of the record writers
			RegularPactTask.initOutputWriters(this.eventualOutputs);

			// start all chained tasks
			RegularPactTask.openChainedTasks(this.chainedTasks, this);
			
			// get input splits to read
			final Iterator<InputSplit> splitIterator = getInputSplits();
			
			// for each assigned input split
			while (!this.taskCanceled && splitIterator.hasNext())
			{
				// get start and end
				final InputSplit split = splitIterator.next();
				
				OT record = serializer.createInstance();
	
				if (LOG.isDebugEnabled()) {
					LOG.debug(getLogString("Opening input split " + split.toString()));
				}
				
				final InputFormat<OT, InputSplit> format = this.format;
			
				// open input format
				format.open(split);
	
				if (LOG.isDebugEnabled()) {
					LOG.debug(getLogString("Starting to read input from split " + split.toString()));
				}
				
				try {
					// ======= special-case the Record, to help the JIT and avoid some casts ======
					if (record.getClass() == Record.class) {
						Record typedRecord = (Record) record;
						@SuppressWarnings("unchecked")
						final InputFormat<Record, InputSplit> inFormat = (InputFormat<Record, InputSplit>) format;
						
						if (this.output instanceof RecordOutputCollector) {
							// Record going directly into network channels
							final RecordOutputCollector output = (RecordOutputCollector) this.output;
							while (!this.taskCanceled && !inFormat.reachedEnd()) {
								// build next pair and ship pair if it is valid
								typedRecord.clear();
								Record returnedRecord = null;
								if ((returnedRecord = inFormat.nextRecord(typedRecord)) != null) {
									output.collect(returnedRecord);
								}
							}
						} else if (this.output instanceof ChainedCollectorMapDriver) {
							// Record going to a chained map task
							@SuppressWarnings("unchecked")
							final ChainedCollectorMapDriver<Record, ?> output = (ChainedCollectorMapDriver<Record, ?>) this.output;
							
							// as long as there is data to read
							while (!this.taskCanceled && !inFormat.reachedEnd()) {
								// build next pair and ship pair if it is valid
								typedRecord.clear();
								if ((typedRecord = inFormat.nextRecord(typedRecord)) != null) {
									// This is where map of UDF gets called
									output.collect(typedRecord);
								}
							}
						} else {
							// Record going to some other chained task
							@SuppressWarnings("unchecked")
							final Collector<Record> output = (Collector<Record>) this.output;
							// as long as there is data to read
							while (!this.taskCanceled && !inFormat.reachedEnd()) {
								// build next pair and ship pair if it is valid
								typedRecord.clear();
								if ((typedRecord = inFormat.nextRecord(typedRecord)) != null){
									output.collect(typedRecord);
								}
							}
						}
					} else {
						// general types. we make a case distinction here for the common cases, in order to help
						// JIT method inlining
						if (this.output instanceof OutputCollector) {
							final OutputCollector<OT> output = (OutputCollector<OT>) this.output;
							// as long as there is data to read
							while (!this.taskCanceled && !format.reachedEnd()) {
								// build next pair and ship pair if it is valid
								if ((record = format.nextRecord(record)) != null) {
									output.collect(record);
								}
							}
						} else if (this.output instanceof ChainedCollectorMapDriver) {
							@SuppressWarnings("unchecked")
							final ChainedCollectorMapDriver<OT, ?> output = (ChainedCollectorMapDriver<OT, ?>) this.output;
							// as long as there is data to read
							while (!this.taskCanceled && !format.reachedEnd()) {
								// build next pair and ship pair if it is valid
								if ((record = format.nextRecord(record)) != null) {
									output.collect(record);
								}
							}
						} else {
							final Collector<OT> output = this.output;
							// as long as there is data to read
							while (!this.taskCanceled && !format.reachedEnd()) {
								// build next pair and ship pair if it is valid
								if ((record = format.nextRecord(record)) != null) {
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
			
			// Merge and report accumulators
			RegularPactTask.reportAndClearAccumulators(getEnvironment(),
					new HashMap<String, Accumulator<?,?>>(), chainedTasks);
		}
		catch (Exception ex) {
			// close the input, but do not report any exceptions, since we already have another root cause
			try {
				this.format.close();
			} catch (Throwable t) {}
			
			RegularPactTask.cancelChainedTasks(this.chainedTasks);
			
			ex = ExceptionInChainedStubException.exceptionUnwrap(ex);

			if (ex instanceof CancelTaskException) {
				// forward canceling exception
				throw ex;
			}
			else if (!this.taskCanceled) {
				// drop exception, if the task was canceled
				RegularPactTask.logAndThrowException(ex, this);
			}
		}

		if (!this.taskCanceled) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(getLogString("Finished data source operator"));
			}
		}
		else {
			if (LOG.isDebugEnabled()) {
				LOG.debug(getLogString("Data source operator cancelled"));
			}
		}
	}

	@Override
	public void cancel() throws Exception {
		this.taskCanceled = true;
		if (LOG.isDebugEnabled()) {
			LOG.debug(getLogString("Cancelling data source operator"));
		}
	}
	
	/**
	 * Sets the class-loader to be used to load the user code.
	 * 
	 * @param cl The class-loader to be used to load the user code.
	 */
	public void setUserCodeClassLoader(ClassLoader cl) {
		this.userCodeClassLoader = cl;
	}

	/**
	 * Initializes the InputFormat implementation and configuration.
	 * 
	 * @throws RuntimeException
	 *         Throws if instance of InputFormat implementation can not be
	 *         obtained.
	 */
	private void initInputFormat(ClassLoader cl) {
		// instantiate the stub
		@SuppressWarnings("unchecked")
		Class<InputFormat<OT, InputSplit>> superClass = (Class<InputFormat<OT, InputSplit>>) (Class<?>) InputFormat.class;
		this.format = RegularPactTask.instantiateUserCode(this.config, cl, superClass);
		
		// configure the stub. catch exceptions here extra, to report them as originating from the user code 
		try {
			this.format.configure(this.config.getStubParameters());
		}
		catch (Throwable t) {
			throw new RuntimeException("The user defined 'configure()' method caused an error: " + t.getMessage(), t);
		}
		
		// get the factory for the type serializer
		this.serializerFactory = this.config.getOutputSerializer(cl);
	}

	/**
	 * Creates a writer for each output. Creates an OutputCollector which forwards its input to all writers.
	 * The output collector applies the configured shipping strategy.
	 */
	private void initOutputs(ClassLoader cl) throws Exception {
		this.chainedTasks = new ArrayList<ChainedDriver<?, ?>>();
		this.eventualOutputs = new ArrayList<BufferWriter>();
		this.output = RegularPactTask.initOutputs(this, cl, this.config, this.chainedTasks, this.eventualOutputs);
	}
	
	// ------------------------------------------------------------------------
	//                              Input Split creation
	// ------------------------------------------------------------------------
	

	@Override
	public InputSplit[] computeInputSplits(int requestedMinNumber) throws Exception {
		// we have to be sure that the format is instantiated at this point
		if (this.format == null) {
			throw new IllegalStateException("BUG: Input format hast not been instantiated, yet.");
		}
		return this.format.createInputSplits(requestedMinNumber);
	}


	@SuppressWarnings("unchecked")
	@Override
	public Class<InputSplit> getInputSplitType() {
		// we have to be sure that the format is instantiated at this point
		if (this.format == null) {
			throw new IllegalStateException("BUG: Input format hast not been instantiated, yet.");
		}
		
		return (Class<InputSplit>) this.format.getInputSplitType();
	}
	
	// ------------------------------------------------------------------------
	//                       Control of Parallelism
	// ------------------------------------------------------------------------
	

	@Override
	public int getMinimumNumberOfSubtasks() {
		return 1;
	}


	@Override
	public int getMaximumNumberOfSubtasks() {
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
	private String getLogString(String message) {
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
	private String getLogString(String message, String taskName) {
		return RegularPactTask.constructLogString(message, taskName, this);
	}
}
