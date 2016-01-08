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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.api.reader.MutableReader;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.chaining.ExceptionInChainedStubException;
import org.apache.flink.runtime.operators.sort.UnilateralSortMerger;
import org.apache.flink.runtime.operators.util.CloseableInputProvider;
import org.apache.flink.runtime.operators.util.ReaderIterator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.MutableObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * DataSinkTask which is executed by a task manager. The task hands the data to an output format.
 * 
 * @see OutputFormat
 */
public class DataSinkTask<IT> extends AbstractInvokable {
	
	// Obtain DataSinkTask Logger
	private static final Logger LOG = LoggerFactory.getLogger(DataSinkTask.class);

	// --------------------------------------------------------------------------------------------
	
	// OutputFormat instance. volatile, because the asynchronous canceller may access it
	private volatile OutputFormat<IT> format;

	private MutableReader<?> inputReader;

	// input reader
	private MutableObjectIterator<IT> reader;

	// The serializer for the input type
	private TypeSerializerFactory<IT> inputTypeSerializerFactory;
	
	// local strategy
	private CloseableInputProvider<IT> localStrategy;

	// task configuration
	private TaskConfig config;
	
	// cancel flag
	private volatile boolean taskCanceled;
	
	private volatile boolean cleanupCalled;

	@Override
	public void registerInputOutput() {
		if (LOG.isDebugEnabled()) {
			LOG.debug(getLogString("Start registering input and output"));
		}

		// initialize OutputFormat
		initOutputFormat();
		
		// initialize input readers
		try {
			initInputReaders();
		} catch (Exception e) {
			throw new RuntimeException("Initializing the input streams failed" +
					(e.getMessage() == null ? "." : ": " + e.getMessage()), e);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug(getLogString("Finished registering input and output"));
		}
	}


	@Override
	public void invoke() throws Exception
	{
		if (LOG.isDebugEnabled()) {
			LOG.debug(getLogString("Starting data sink operator"));
		}

		ExecutionConfig executionConfig;
		try {
			ExecutionConfig c = (ExecutionConfig) InstantiationUtil.readObjectFromConfig(
					getJobConfiguration(),
					ExecutionConfig.CONFIG_KEY,
					getUserCodeClassLoader());
			if (c != null) {
				executionConfig = c;
			} else {
				LOG.warn("The execution config returned by the configuration was null");
				executionConfig = new ExecutionConfig();
			}
		} catch (IOException e) {
			throw new RuntimeException("Could not load ExecutionConfig from Job Configuration: " + e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Could not load ExecutionConfig from Job Configuration: " + e);
		}
		boolean objectReuseEnabled = executionConfig.isObjectReuseEnabled();
		
		try {
			
			// initialize local strategies
			MutableObjectIterator<IT> input1;
			switch (this.config.getInputLocalStrategy(0)) {
			case NONE:
				// nothing to do
				localStrategy = null;
				input1 = reader;
				break;
			case SORT:
				// initialize sort local strategy
				try {
					// get type comparator
					TypeComparatorFactory<IT> compFact = this.config.getInputComparator(0,
							getUserCodeClassLoader());
					if (compFact == null) {
						throw new Exception("Missing comparator factory for local strategy on input " + 0);
					}
					
					// initialize sorter
					UnilateralSortMerger<IT> sorter = new UnilateralSortMerger<IT>(
							getEnvironment().getMemoryManager(), 
							getEnvironment().getIOManager(),
							this.reader, this, this.inputTypeSerializerFactory, compFact.createComparator(),
							this.config.getRelativeMemoryInput(0), this.config.getFilehandlesInput(0),
							this.config.getSpillingThresholdInput(0));
					
					this.localStrategy = sorter;
					input1 = sorter.getIterator();
				} catch (Exception e) {
					throw new RuntimeException("Initializing the input processing failed" +
							(e.getMessage() == null ? "." : ": " + e.getMessage()), e);
				}
				break;
			default:
				throw new RuntimeException("Invalid local strategy for DataSinkTask");
			}
			
			// read the reader and write it to the output
			
			final TypeSerializer<IT> serializer = this.inputTypeSerializerFactory.getSerializer();
			final MutableObjectIterator<IT> input = input1;
			final OutputFormat<IT> format = this.format;


			// check if task has been canceled
			if (this.taskCanceled) {
				return;
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug(getLogString("Starting to produce output"));
			}

			// open
			format.open(this.getEnvironment().getIndexInSubtaskGroup(), this.getEnvironment().getNumberOfSubtasks());

			if (objectReuseEnabled) {
				IT record = serializer.createInstance();

				// work!
				while (!this.taskCanceled && ((record = input.next(record)) != null)) {
					format.writeRecord(record);
				}
			} else {
				IT record;

				// work!
				while (!this.taskCanceled && ((record = input.next()) != null)) {
					format.writeRecord(record);
				}
			}
			
			// close. We close here such that a regular close throwing an exception marks a task as failed.
			if (!this.taskCanceled) {
				this.format.close();
				this.format = null;
			}
		}
		catch (Exception ex) {
			
			// make a best effort to clean up
			try {
				if (!cleanupCalled && format instanceof CleanupWhenUnsuccessful) {
					cleanupCalled = true;
					((CleanupWhenUnsuccessful) format).tryCleanupOnError();
				}
			}
			catch (Throwable t) {
				LOG.error("Cleanup on error failed.", t);
			}
			
			ex = ExceptionInChainedStubException.exceptionUnwrap(ex);

			if (ex instanceof CancelTaskException) {
				// forward canceling exception
				throw ex;
			}
			// drop, if the task was canceled
			else if (!this.taskCanceled) {
				if (LOG.isErrorEnabled()) {
					LOG.error(getLogString("Error in user code: " + ex.getMessage()), ex);
				}
				throw ex;
			}
		}
		finally {
			if (this.format != null) {
				// close format, if it has not been closed, yet.
				// This should only be the case if we had a previous error, or were canceled.
				try {
					this.format.close();
				}
				catch (Throwable t) {
					if (LOG.isWarnEnabled()) {
						LOG.warn(getLogString("Error closing the output format"), t);
					}
				}
			}
			// close local strategy if necessary
			if (localStrategy != null) {
				try {
					this.localStrategy.close();
				} catch (Throwable t) {
					LOG.error("Error closing local strategy", t);
				}
			}

			RegularPactTask.clearReaders(new MutableReader[]{inputReader});
		}

		if (!this.taskCanceled) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(getLogString("Finished data sink operator"));
			}
		}
		else {
			if (LOG.isDebugEnabled()) {
				LOG.debug(getLogString("Data sink operator cancelled"));
			}
		}
	}

	@Override
	public void cancel() throws Exception {
		this.taskCanceled = true;
		OutputFormat<IT> format = this.format;
		if (format != null) {
			try {
				this.format.close();
			} catch (Throwable t) {}
			
			// make a best effort to clean up
			try {
				if (!cleanupCalled && format instanceof CleanupWhenUnsuccessful) {
					cleanupCalled = true;
					((CleanupWhenUnsuccessful) format).tryCleanupOnError();
				}
			}
			catch (Throwable t) {
				LOG.error("Cleanup on error failed.", t);
			}
		}
		
		if (LOG.isDebugEnabled()) {
			LOG.debug(getLogString("Cancelling data sink operator"));
		}
	}
	
	/**
	 * Initializes the OutputFormat implementation and configuration.
	 * 
	 * @throws RuntimeException
	 *         Throws if instance of OutputFormat implementation can not be
	 *         obtained.
	 */
	private void initOutputFormat() {
		ClassLoader userCodeClassLoader = getUserCodeClassLoader();
		// obtain task configuration (including stub parameters)
		Configuration taskConf = getTaskConfiguration();
		this.config = new TaskConfig(taskConf);

		try {
			this.format = config.<OutputFormat<IT>>getStubWrapper(userCodeClassLoader).getUserCodeObject(OutputFormat.class, userCodeClassLoader);

			// check if the class is a subclass, if the check is required
			if (!OutputFormat.class.isAssignableFrom(this.format.getClass())) {
				throw new RuntimeException("The class '" + this.format.getClass().getName() + "' is not a subclass of '" + 
						OutputFormat.class.getName() + "' as is required.");
			}
		}
		catch (ClassCastException ccex) {
			throw new RuntimeException("The stub class is not a proper subclass of " + OutputFormat.class.getName(), ccex);
		}

		Thread thread = Thread.currentThread();
		ClassLoader original = thread.getContextClassLoader();
		// configure the stub. catch exceptions here extra, to report them as originating from the user code 
		try {
			thread.setContextClassLoader(userCodeClassLoader);
			this.format.configure(this.config.getStubParameters());
		}
		catch (Throwable t) {
			throw new RuntimeException("The user defined 'configure()' method in the Output Format caused an error: " 
				+ t.getMessage(), t);
		}
		finally {
			thread.setContextClassLoader(original);
		}
	}

	/**
	 * Initializes the input readers of the DataSinkTask.
	 * 
	 * @throws RuntimeException
	 *         Thrown in case of invalid task input configuration.
	 */
	@SuppressWarnings("unchecked")
	private void initInputReaders() throws Exception {
		int numGates = 0;
		//  ---------------- create the input readers ---------------------
		// in case where a logical input unions multiple physical inputs, create a union reader
		final int groupSize = this.config.getGroupSize(0);
		numGates += groupSize;
		if (groupSize == 1) {
			// non-union case
			inputReader = new MutableRecordReader<DeserializationDelegate<IT>>(getEnvironment().getInputGate(0));
		} else if (groupSize > 1){
			// union case
			inputReader = new MutableRecordReader<IOReadableWritable>(new UnionInputGate(getEnvironment().getAllInputGates()));
		} else {
			throw new Exception("Illegal input group size in task configuration: " + groupSize);
		}

		final AccumulatorRegistry accumulatorRegistry = getEnvironment().getAccumulatorRegistry();
		final AccumulatorRegistry.Reporter reporter = accumulatorRegistry.getReadWriteReporter();

		inputReader.setReporter(reporter);

		this.inputTypeSerializerFactory = this.config.getInputSerializer(0, getUserCodeClassLoader());
		@SuppressWarnings({ "rawtypes" })
		final MutableObjectIterator<?> iter = new ReaderIterator(inputReader, this.inputTypeSerializerFactory.getSerializer());
		this.reader = (MutableObjectIterator<IT>)iter;

		// final sanity check
		if (numGates != this.config.getNumInputs()) {
			throw new Exception("Illegal configuration: Number of input gates and group sizes are not consistent.");
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
	private String getLogString(String message) {
		return RegularPactTask.constructLogString(message, this.getEnvironment().getTaskName(), this);
	}
}
