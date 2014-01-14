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

import eu.stratosphere.pact.runtime.task.chaining.ExceptionInChainedStubException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.io.FileOutputFormat;
import eu.stratosphere.api.common.io.FileOutputFormat.OutputDirectoryMode;
import eu.stratosphere.api.common.io.OutputFormat;
import eu.stratosphere.api.common.typeutils.TypeComparatorFactory;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.FileSystem.WriteMode;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.execution.CancelTaskException;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.runtime.io.api.MutableReader;
import eu.stratosphere.runtime.io.api.MutableRecordReader;
import eu.stratosphere.runtime.io.api.MutableUnionRecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.pact.runtime.plugable.DeserializationDelegate;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.ReaderIterator;
import eu.stratosphere.pact.runtime.task.util.RecordReaderIterator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.MutableObjectIterator;

/**
 * DataSinkTask which is executed by a Nephele task manager.
 * The task hands the data to an output format.
 * 
 * @see OutputFormat
 */
public class DataSinkTask<IT> extends AbstractOutputTask {
	
	public static final String DEGREE_OF_PARALLELISM_KEY = "sink.dop";
	
	// Obtain DataSinkTask Logger
	private static final Log LOG = LogFactory.getLog(DataSinkTask.class);

	// --------------------------------------------------------------------------------------------
	
	// OutputFormat instance. volatile, because the asynchronous canceller may access it
	private volatile OutputFormat<IT> format;
	
	// input reader
	private MutableObjectIterator<IT> reader;
	
	// input iterator
	private MutableObjectIterator<IT> input;
	
	// The serializer for the input type
	private TypeSerializerFactory<IT> inputTypeSerializerFactory;
	
	// local strategy
	private CloseableInputProvider<IT> localStrategy;

	// task configuration
	private TaskConfig config;
	
	// class loader for user code
	private ClassLoader userCodeClassLoader;

	// cancel flag
	private volatile boolean taskCanceled;
	

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
				e.getMessage() == null ? "." : ": " + e.getMessage(), e);
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
		
		try {
			
			// initialize local strategies
			switch (this.config.getInputLocalStrategy(0)) {
			case NONE:
				// nothing to do
				localStrategy = null;
				input = reader;
				break;
			case SORT:
				// initialize sort local strategy
				try {
					// get type comparator
					TypeComparatorFactory<IT> compFact = this.config.getInputComparator(0, this.userCodeClassLoader);
					if (compFact == null) {
						throw new Exception("Missing comparator factory for local strategy on input " + 0);
					}
					
					// initialize sorter
					UnilateralSortMerger<IT> sorter = new UnilateralSortMerger<IT>(
							getEnvironment().getMemoryManager(), 
							getEnvironment().getIOManager(),
							this.reader, this, this.inputTypeSerializerFactory, compFact.createComparator(),
							this.config.getMemoryInput(0), this.config.getFilehandlesInput(0),
							this.config.getSpillingThresholdInput(0));
					
					this.localStrategy = sorter;
					this.input = sorter.getIterator();
				} catch (Exception e) {
					throw new RuntimeException("Initializing the input processing failed" +
						e.getMessage() == null ? "." : ": " + e.getMessage(), e);
				}
				break;
			default:
				throw new RuntimeException("Invalid local strategy for DataSinkTask");
			}
			
			// read the reader and write it to the output
			
			final TypeSerializer<IT> serializer = this.inputTypeSerializerFactory.getSerializer();
			final MutableObjectIterator<IT> input = this.input;
			final OutputFormat<IT> format = this.format;
			
			
			IT record = serializer.createInstance();
			
			// check if task has been canceled
			if (this.taskCanceled) {
				return;
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug(getLogString("Starting to produce output"));
			}

			// open
			format.open(this.getEnvironment().getIndexInSubtaskGroup(), this.getEnvironment().getCurrentNumberOfSubtasks());

			// work!
			while (!this.taskCanceled && ((record = input.next(record)) != null)) {
				format.writeRecord(record);
			}
			
			// close. We close here such that a regular close throwing an exception marks a task as failed.
			if (!this.taskCanceled) {
				this.format.close();
				this.format = null;
			}
		}
		catch (Exception ex) {
			ex = ExceptionInChainedStubException.exceptionUnwrap(ex);

			if (ex instanceof CancelTaskException) {
				// forward canceling exception
				throw ex;
			}
			// drop, if the task was canceled
			else if (!this.taskCanceled) {
				if (LOG.isErrorEnabled())
					LOG.error(getLogString("Error in user code: " + ex.getMessage()), ex);
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
						LOG.warn(getLogString("Error closing the ouput format."), t);
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
		}
		
		if (LOG.isDebugEnabled()) {
			LOG.debug(getLogString("Cancelling data sink operator"));
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
	 * Initializes the OutputFormat implementation and configuration.
	 * 
	 * @throws RuntimeException
	 *         Throws if instance of OutputFormat implementation can not be
	 *         obtained.
	 */
	private void initOutputFormat() {
		if (this.userCodeClassLoader == null) {
			try {
				this.userCodeClassLoader = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			} catch (IOException ioe) {
				throw new RuntimeException("Library cache manager could not be instantiated.", ioe);
			}
		}
		// obtain task configuration (including stub parameters)
		Configuration taskConf = getTaskConfiguration();
		taskConf.setClassLoader(this.userCodeClassLoader);
		this.config = new TaskConfig(taskConf);

		try {
			this.format = config.<OutputFormat<IT>>getStubWrapper(this.userCodeClassLoader).getUserCodeObject(OutputFormat.class, this.userCodeClassLoader);

			// check if the class is a subclass, if the check is required
			if (!OutputFormat.class.isAssignableFrom(this.format.getClass())) {
				throw new RuntimeException("The class '" + this.format.getClass().getName() + "' is not a subclass of '" + 
						OutputFormat.class.getName() + "' as is required.");
			}
		}
		catch (ClassCastException ccex) {
			throw new RuntimeException("The stub class is not a proper subclass of " + OutputFormat.class.getName(), ccex);
		}
		
		// configure the stub. catch exceptions here extra, to report them as originating from the user code 
		try {
			this.format.configure(this.config.getStubParameters());
		}
		catch (Throwable t) {
			throw new RuntimeException("The user defined 'configure()' method in the Output Format caused an error: " 
				+ t.getMessage(), t);
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
		
		MutableReader<?> inputReader;
		
		int numGates = 0;
		//  ---------------- create the input readers ---------------------
		// in case where a logical input unions multiple physical inputs, create a union reader
		final int groupSize = this.config.getGroupSize(0);
		numGates += groupSize;
		if (groupSize == 1) {
			// non-union case
			inputReader = new MutableRecordReader<DeserializationDelegate<IT>>(this);
		} else if (groupSize > 1){
			// union case
			
			MutableRecordReader<IOReadableWritable>[] readers = new MutableRecordReader[groupSize];
			for (int j = 0; j < groupSize; ++j) {
				readers[j] = new MutableRecordReader<IOReadableWritable>(this);
			}
			inputReader = new MutableUnionRecordReader<IOReadableWritable>(readers);
		} else {
			throw new Exception("Illegal input group size in task configuration: " + groupSize);
		}
		
		this.inputTypeSerializerFactory = this.config.getInputSerializer(0, this.userCodeClassLoader);
		
		if (this.inputTypeSerializerFactory.getDataType() == Record.class) {
			// record specific deserialization
			MutableReader<Record> reader = (MutableReader<Record>) inputReader;
			this.reader = (MutableObjectIterator<IT>)new RecordReaderIterator(reader);
		} else {
			// generic data type serialization
			MutableReader<DeserializationDelegate<?>> reader = (MutableReader<DeserializationDelegate<?>>) inputReader;
			@SuppressWarnings({ "rawtypes" })
			final MutableObjectIterator<?> iter = new ReaderIterator(reader, this.inputTypeSerializerFactory.getSerializer());
			this.reader = (MutableObjectIterator<IT>)iter;
		}
		
		// final sanity check
		if (numGates != this.config.getNumInputs()) {
			throw new Exception("Illegal configuration: Number of input gates and group sizes are not consistent.");
		}
	}
	
	// ------------------------------------------------------------------------
	//                     Degree of parallelism & checks
	// ------------------------------------------------------------------------
	

	@Override
	public int getMaximumNumberOfSubtasks() {
		if (!(this.format instanceof FileOutputFormat<?>)) {
			return -1;
		}
		
		final FileOutputFormat<?> fileOutputFormat = (FileOutputFormat<?>) this.format;
		
		// ----------------- This code applies only to file inputs ------------------
		
		final Path path = fileOutputFormat.getOutputFilePath();
		final WriteMode writeMode = fileOutputFormat.getWriteMode();
		final OutputDirectoryMode outDirMode = fileOutputFormat.getOutputDirectoryMode();

		// Prepare output path and determine max DOP		
		try {
			
			int dop = getTaskConfiguration().getInteger(DEGREE_OF_PARALLELISM_KEY, -1);
			final FileSystem fs = path.getFileSystem();
			
			if(dop == 1 && outDirMode == OutputDirectoryMode.PARONLY) {
				// output is not written in parallel and should be written to a single file.
				
				if(fs.isDistributedFS()) {
					// prepare distributed output path
					if(!fs.initOutPathDistFS(path, writeMode, false)) {
						// output preparation failed! Cancel task.
						throw new IOException("Output path could not be initialized.");
					}
				}
				
				return 1;
				
			} else {
				// output should be written to a directory
				
				if(fs.isDistributedFS()) {
					// only distributed file systems can be initialized at start-up time.
					if(!fs.initOutPathDistFS(path, writeMode, true)) {
						throw new IOException("Output directory could not be created.");
					}
				}
				
				return -1;
				
			}
		}
		catch (IOException e) {
			LOG.error("Could not access the file system to detemine the status of the output.", e);
			throw new RuntimeException("I/O Error while accessing file", e);
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
