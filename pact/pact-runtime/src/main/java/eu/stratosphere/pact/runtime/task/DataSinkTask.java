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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.BipartiteDistributionPattern;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.PointwiseDistributionPattern;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.OutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.task.util.NepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * DataSinkTask which is executed by a Nephele task manager.
 * The task hands the data to an output format.
 * 
 * @see eu.stratosphere.pact.common.io.OutputFormat
 * 
 * @author Fabian Hueske
 */
public class DataSinkTask extends AbstractOutputTask
{
	public static final String DEGREE_OF_PARALLELISM_KEY = "pact.sink.dop";
	
	public static final String SORT_ORDER = "sink.sort.order";
	
	// Obtain DataSinkTask Logger
	private static final Log LOG = LogFactory.getLog(DataSinkTask.class);

	// --------------------------------------------------------------------------------------------
	
	// input reader
	private MutableObjectIterator<PactRecord> reader;

	// OutputFormat instance
	private OutputFormat format;

	// task configuration
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

		// initialize OutputFormat
		initOutputFormat();
		// initialize input reader
		initInputReader();

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

		final MutableObjectIterator<PactRecord> reader = this.reader;
		final OutputFormat format = this.format;
		final PactRecord record = new PactRecord();
		
		try {
			// check if task has been canceled
			if (this.taskCanceled) {
				return;
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug(getLogString("Starting to produce output"));
			}

			// open
			format.open(this.getEnvironment().getIndexInSubtaskGroup() + 1);

			// work
			while (!this.taskCanceled && reader.next(record))
			{
				format.writeRecord(record);
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
				// This should only be the case if we had a previous error, or were cancelled.
				try {
					this.format.close();
				}
				catch (Throwable t) {
					if (LOG.isWarnEnabled())
						LOG.warn(getLogString("Error closing the ouput format."), t);
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
		if (LOG.isWarnEnabled())
			LOG.warn(getLogString("Cancelling PACT code"));
	}

	/**
	 * Initializes the OutputFormat implementation and configuration.
	 * 
	 * @throws RuntimeException
	 *         Throws if instance of OutputFormat implementation can not be
	 *         obtained.
	 */
	private void initOutputFormat()
	{
		// obtain task configuration (including stub parameters)
		this.config = new TaskConfig(getTaskConfiguration());

		// obtain stub implementation class
		try {
			ClassLoader cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			Class<? extends OutputFormat> formatClass = this.config.getStubClass(OutputFormat.class, cl);
			
			// obtain instance of stub implementation
			this.format = InstantiationUtil.instantiate(formatClass, OutputFormat.class);
		}
		catch (IOException ioe) {
			throw new RuntimeException("Library cache manager could not be instantiated.", ioe);
		}
		catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("OutputFormat implementation class was not found.", cnfe);
		}
		catch (ClassCastException ccex) {
			throw new RuntimeException("Format format class is not a proper subclass of " + OutputFormat.class.getName(), ccex); 
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
	 * Initializes the input reader of the DataSinkTask.
	 * 
	 * @throws RuntimeException
	 *         Thrown if no input ship strategy was provided.
	 */
	private void initInputReader()
	{
		// determine distribution pattern for reader from input ship strategy
		DistributionPattern dp = null;
		switch (this.config.getInputShipStrategy(0)) {
		case FORWARD:
			// forward requires Pointwise DP
			dp = new PointwiseDistributionPattern();
			break;
		case PARTITION_RANGE:
			dp = new BipartiteDistributionPattern();
			break;
		default:
			throw new RuntimeException("No valid input ship strategy provided for DataSinkTask.");
		}

		// create reader
		this.reader = new NepheleReaderIterator(new MutableRecordReader<PactRecord>(this, dp));
	}
	
	// ------------------------------------------------------------------------
	//                     Degree of parallelism & checks
	// ------------------------------------------------------------------------
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumNumberOfSubtasks()
	{
		if (!(this.format instanceof FileOutputFormat)) {
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
					// path points to an existing file. delete it, to prevent errors appearing
					// when overwriting the file (HDFS causes non-deterministic errors there)
					fs.delete(path, false);
					return 1;
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
			// any other kind of I/O exception: we assume only a degree of one here
			return 1;
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
