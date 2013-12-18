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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.io.FileOutputFormat;
import eu.stratosphere.api.io.OutputFormat;
import eu.stratosphere.api.typeutils.TypeComparatorFactory;
import eu.stratosphere.api.typeutils.TypeSerializer;
import eu.stratosphere.api.typeutils.TypeSerializerFactory;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileStatus;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.MutableReader;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.MutableUnionRecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.pact.runtime.plugable.DeserializationDelegate;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordSerializer;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.NepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.PactRecordNepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.util.MutableObjectIterator;

/**
 * DataSinkTask which is executed by a Nephele task manager.
 * The task hands the data to an output format.
 * 
 * @see eu.eu.stratosphere.pact.common.generic.io.OutputFormat
 */
public class DataSinkTask<IT> extends AbstractOutputTask
{
	public static final String DEGREE_OF_PARALLELISM_KEY = "pact.sink.dop";
	
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
	private TypeSerializer<IT> inputTypeSerializer;
	
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
		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("Start registering input and output"));

		// initialize OutputFormat
		initOutputFormat();
		
		// initialize input readers
		try {
			initInputReaders();
		} catch (Exception e) {
			throw new RuntimeException("Initializing the input streams failed" +
				e.getMessage() == null ? "." : ": " + e.getMessage(), e);
		}

		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("Finished registering input and output"));
	}


	@Override
	public void invoke() throws Exception
	{
		if (LOG.isInfoEnabled())
			LOG.info(getLogString("Start PACT code"));
		
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
							this.reader, this, this.inputTypeSerializer, compFact.createComparator(),
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
			final MutableObjectIterator<IT> input = this.input;
			final OutputFormat<IT> format = this.format;
			final IT record = this.inputTypeSerializer.createInstance();
			
			// check if task has been canceled
			if (this.taskCanceled) {
				return;
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug(getLogString("Starting to produce output"));
			}

			// open
			format.open(this.getEnvironment().getIndexInSubtaskGroup() + 1);

			// work!
			// special case the pact record / file variant
			if (record.getClass() == PactRecord.class && format instanceof eu.stratosphere.api.record.io.FileOutputFormat) {
				@SuppressWarnings("unchecked")
				final MutableObjectIterator<PactRecord> pi = (MutableObjectIterator<PactRecord>) input;
				final PactRecord pr = (PactRecord) record;
				final eu.stratosphere.api.record.io.FileOutputFormat pf = (eu.stratosphere.api.record.io.FileOutputFormat) format;
				while (!this.taskCanceled && pi.next(pr)) {
					pf.writeRecord(pr);
				}
			} else {
				while (!this.taskCanceled && input.next(record)) {
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
			// drop, if the task was canceled
			if (!this.taskCanceled) {
				if (LOG.isErrorEnabled())
					LOG.error(getLogString("Error in Pact user code: " + ex.getMessage()), ex);
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
					if (LOG.isWarnEnabled())
						LOG.warn(getLogString("Error closing the ouput format."), t);
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
			if (LOG.isDebugEnabled())
				LOG.debug(getLogString("Finished producing output"));

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
		OutputFormat<IT> format = this.format;
		if (format != null) {
			try {
				this.format.close();
			} catch (Throwable t) {}
		}
		
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
		
		final TypeSerializerFactory<IT> serializerFactory = this.config.getInputSerializer(0, this.userCodeClassLoader);
		this.inputTypeSerializer = serializerFactory.getSerializer();
		
		if (this.inputTypeSerializer.getClass() == PactRecordSerializer.class) {
			// pact record specific deserialization
			MutableReader<PactRecord> reader = (MutableReader<PactRecord>) inputReader;
			this.reader = (MutableObjectIterator<IT>)new PactRecordNepheleReaderIterator(reader);
		} else {
			// generic data type serialization
			MutableReader<DeserializationDelegate<?>> reader = (MutableReader<DeserializationDelegate<?>>) inputReader;
			@SuppressWarnings({ "rawtypes" })
			final MutableObjectIterator<?> iter = new NepheleReaderIterator(reader, this.inputTypeSerializer);
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
	public int getMaximumNumberOfSubtasks()
	{
		if (!(this.format instanceof FileOutputFormat<?>)) {
			return -1;
		}
		
		// ----------------- This code applies only to file inputs ------------------
		
		final String pathName = this.config.getStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, null);
		final Path path;
		
		if (pathName == null) {
			return 0;
		}
		
		try {
			path = new Path(pathName);
		}
		catch (Throwable t) {
			return 0;
		}

		// Check if the path is valid
		try {
			final FileSystem fs = path.getFileSystem();
			try {
				final FileStatus f = fs.getFileStatus(path);
				if (f == null) {
					return 1;
				}
				// If the path points to a directory we allow an infinity number of subtasks
				if (f.isDir()) {
					return -1;
				}
				else {
					// path points to an existing file. delete it to be able to replace the
					// file with a directory
					fs.delete(path, false);
					int dop = getTaskConfiguration().getInteger(DEGREE_OF_PARALLELISM_KEY, -1);
					if (dop == 1) {
						// a none existing file and a degree of parallelism that is one
						return 1;
					} else {
						// a degree of parallelism greater one, or an unspecified one. in all cases, create a directory
						// the output
						fs.mkdirs(path);
						return -1;
					}
				}
			}
			catch (FileNotFoundException fnfex) {
				// The exception is thrown if the requested file/directory does not exist.
				// if the degree of parallelism is > 1, we create a directory for this path
				int dop = getTaskConfiguration().getInteger(DEGREE_OF_PARALLELISM_KEY, -1);
				if (dop == 1) {
					// a none existing file and a degree of parallelism that is one
					return 1;
				}

				// a degree of parallelism greater one, or an unspecified one. in all cases, create a directory
				// the output
				fs.mkdirs(path);
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
