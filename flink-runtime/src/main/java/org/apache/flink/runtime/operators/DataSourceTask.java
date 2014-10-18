/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.api.BufferWriter;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.operators.chaining.ChainedCollectorMapDriver;
import org.apache.flink.runtime.operators.chaining.ChainedDriver;
import org.apache.flink.runtime.operators.chaining.ExceptionInChainedStubException;
import org.apache.flink.runtime.operators.shipping.OutputCollector;
import org.apache.flink.runtime.operators.shipping.RecordOutputCollector;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

/**
 * DataSourceTask which is executed by a Nephele task manager. The task reads data and uses an 
 * {@link InputFormat} to create records from the input.
 * 
 * @see org.apache.flink.api.common.io.InputFormat
 */
public class DataSourceTask<OT> extends AbstractInvokable {
	
	private static final Logger LOG = LoggerFactory.getLogger(DataSourceTask.class);

	
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
	
	// cancel flag
	private volatile boolean taskCanceled = false;


	@Override
	public void registerInputOutput() {
		initInputFormat();

		if (LOG.isDebugEnabled()) {
			LOG.debug(getLogString("Start registering input and output"));
		}

		try {
			initOutputs(getUserCodeClassLoader());
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
	 * Initializes the InputFormat implementation and configuration.
l	 * 
	 * @throws RuntimeException
	 *         Throws if instance of InputFormat implementation can not be
	 *         obtained.
	 */
	private void initInputFormat() {
		ClassLoader userCodeClassLoader = getUserCodeClassLoader();
		// obtain task configuration (including stub parameters)
		Configuration taskConf = getTaskConfiguration();
		this.config = new TaskConfig(taskConf);

		try {
			this.format = config.<InputFormat<OT, InputSplit>>getStubWrapper(userCodeClassLoader)
					.getUserCodeObject(InputFormat.class, userCodeClassLoader);

			// check if the class is a subclass, if the check is required
			if (!InputFormat.class.isAssignableFrom(this.format.getClass())) {
				throw new RuntimeException("The class '" + this.format.getClass().getName() + "' is not a subclass of '" +
						InputFormat.class.getName() + "' as is required.");
			}
		}
		catch (ClassCastException ccex) {
			throw new RuntimeException("The stub class is not a proper subclass of " + InputFormat.class.getName(),
					ccex);
		}

		// configure the stub. catch exceptions here extra, to report them as originating from the user code 
		try {
			this.format.configure(this.config.getStubParameters());
		}
		catch (Throwable t) {
			throw new RuntimeException("The user defined 'configure()' method caused an error: " + t.getMessage(), t);
		}
		
		// get the factory for the type serializer
		this.serializerFactory = this.config.getOutputSerializer(userCodeClassLoader);
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
	
	private Iterator<InputSplit> getInputSplits() {

		final InputSplitProvider provider = getEnvironment().getInputSplitProvider();

		return new Iterator<InputSplit>() {

			private InputSplit nextSplit;
			
			private boolean exhausted;

			@Override
			public boolean hasNext() {
				if (exhausted) {
					return false;
				}
				
				if (nextSplit != null) {
					return true;
				}
				
				InputSplit split = provider.getNextInputSplit();
				
				if (split != null) {
					this.nextSplit = split;
					return true;
				}
				else {
					exhausted = true;
					return false;
				}
			}

			@Override
			public InputSplit next() {
				if (this.nextSplit == null && !hasNext()) {
					throw new NoSuchElementException();
				}

				final InputSplit tmp = this.nextSplit;
				this.nextSplit = null;
				return tmp;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
