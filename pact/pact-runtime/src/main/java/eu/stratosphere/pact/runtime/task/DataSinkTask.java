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
import java.util.Comparator;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.BipartiteDistributionPattern;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.PointwiseDistributionPattern;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.OutputFormat;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairDeserializer;
import eu.stratosphere.pact.runtime.serialization.WritableSerializationFactory;
import eu.stratosphere.pact.runtime.sort.SortMerger;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.SimpleCloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * DataSinkTask which is executed by a Nephele task manager.
 * The task hands the data to an output format.
 * 
 * @see eu.stratosphere.pact.common.io.OutputFormat
 * 
 * @author Fabian Hueske
 */
@SuppressWarnings( { "unchecked", "rawtypes" })
public class DataSinkTask extends AbstractOutputTask
{
	public static final String DEGREE_OF_PARALLELISM_KEY = "pact.sink.dop";
	
	public static final String SORT_ORDER = "sink.sort.order";
	
	// Obtain DataSinkTask Logger
	private static final Log LOG = LogFactory.getLog(DataSinkTask.class);

	// input reader
	private RecordReader<KeyValuePair<Key, Value>> reader;

	// OutputFormat instance
	private OutputFormat format;

	// task configuration
	private TaskConfig config;

	// cancel flag
	private volatile boolean taskCanceled = false;
	
	// the memory dedicated to the sorter
	private long availableMemory;

	// maximum number of file handles
	private int maxFileHandles;

	// the fill fraction of the buffers that triggers the spilling
	private float spillThreshold;

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

		final OutputFormat format = this.format;
		
		//Input is wrapped so that it can also be sorted if required
		CloseableInputProvider<KeyValuePair<Key, Value>> inputProvider = null;
		
		try {
			// check if task has been canceled
			if (this.taskCanceled) {
				return;
			}

			inputProvider = obtainInput();
			Iterator<KeyValuePair<Key, Value>> iter = inputProvider.getIterator();
			
			if (LOG.isDebugEnabled()) {
				LOG.debug("Iterator obtained: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

				LOG.debug(getLogString("Starting to produce output"));
			}

			// open
			format.open(this.getEnvironment().getIndexInSubtaskGroup() + 1);

			// work
			while (!this.taskCanceled && iter.hasNext())
			{
				KeyValuePair pair = iter.next();
				format.writeRecord(pair);
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
			if (inputProvider != null) {
				inputProvider.close();
			}
			
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
		this.config = new TaskConfig(getRuntimeConfiguration());

		// obtain stub implementation class
		ClassLoader cl;
		try {
			cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			Class<? extends OutputFormat> formatClass = this.config.getStubClass(OutputFormat.class, cl);
			// obtain instance of stub implementation
			this.format = formatClass.newInstance();
		}
		catch (IOException ioe) {
			throw new RuntimeException("Library cache manager could not be instantiated.", ioe);
		}
		catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("OutputFormat implementation class was not found.", cnfe);
		}
		catch (InstantiationException ie) {
			throw new RuntimeException("OutputFormat implementation could not be instanciated.", ie);
		}
		catch (IllegalAccessException iae) {
			throw new RuntimeException("OutputFormat implementations nullary constructor is not accessible.", iae);
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
	 * Initializes the input reader of the DataSinkTask.
	 * 
	 * @throws RuntimeException
	 *         Thrown if no input ship strategy was provided.
	 */
	private void initInputReader()
	{
		// create RecordDeserializer
		RecordDeserializer<KeyValuePair<Key, Value>> deserializer = new KeyValuePairDeserializer(
			this.format.getKeyType(), format.getValueType());

		// determine distribution pattern for reader from input ship strategy
		DistributionPattern dp = null;
		switch (config.getInputShipStrategy(0)) {
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
		// map has only one input, so we create one reader (id=0).
		this.reader = new RecordReader<KeyValuePair<Key, Value>>(this, deserializer, dp);

		// set up memory and I/O parameters in case of sorting
		this.availableMemory = config.getMemorySize();
		this.maxFileHandles = config.getNumFilehandles();
		this.spillThreshold = config.getSortSpillingTreshold();
	}
	
	/**
	 * Returns an iterator over all k-v pairs of the ReduceTasks input. The
	 * pairs which are returned by the iterator are grouped by their keys.
	 * 
	 * @return A key-grouped iterator over all input key-value pairs.
	 * @throws RuntimeException
	 *         Throws RuntimeException if it is not possible to obtain a
	 *         grouped iterator.
	 */
	private CloseableInputProvider<KeyValuePair<Key, Value>> obtainInput() {
		
		// obtain the MemoryManager of the TaskManager
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		// obtain the IOManager of the TaskManager
		final IOManager ioManager = getEnvironment().getIOManager();

		// obtain input key type
		final Class<Key> keyClass = format.getKeyType();
		// obtain input value type
		final Class<Value> valueClass = format.getValueType();

		// obtain key serializer
		final SerializationFactory<Key> keySerialization = new WritableSerializationFactory<Key>(keyClass);
		// obtain value serializer
		final SerializationFactory<Value> valSerialization = new WritableSerializationFactory<Value>(valueClass);

		// obtain grouped iterator defined by local strategy
		switch (config.getLocalStrategy()) {

		// local strategy is NONE
		// input is already grouped, an iterator that wraps the reader is
		// created and returned
		case NONE: {
			// iterator wraps input reader
			Iterator<KeyValuePair<Key, Value>> iter = new Iterator<KeyValuePair<Key, Value>>() {

				@Override
				public boolean hasNext() {
					return reader.hasNext();
				}

				@Override
				public KeyValuePair<Key, Value> next() {
					try {
						return reader.next();
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void remove() {
				}

			};
			
			return new SimpleCloseableInputProvider<KeyValuePair<Key,Value>>(iter);
		}

			// local strategy is SORT
			// The input is grouped using a sort-merge strategy.
			// An iterator on the sorted pairs is created and returned.
		case SORT: {
			final Order sortOrder = Order.valueOf(getRuntimeConfiguration().getString(SORT_ORDER, ""));
			
			// create a key comparator
			final Comparator<Key> keyComparator;
			
			if(sortOrder == Order.ASCENDING || sortOrder == Order.ANY) {
				keyComparator = new Comparator<Key>() {
					@Override
					public int compare(Key k1, Key k2) {
						return k1.compareTo(k2);
					}
				};
			} else {
				keyComparator = new Comparator<Key>() {
					@Override
					public int compare(Key k1, Key k2) {
						return k2.compareTo(k1);
					}
				};
			}


			try {
				// instantiate a sort-merger
				SortMerger<Key, Value> sortMerger = new UnilateralSortMerger<Key, Value>(memoryManager, ioManager,
					this.availableMemory, this.maxFileHandles, keySerialization,
					valSerialization, keyComparator, reader, this, this.spillThreshold);
				// obtain and return a grouped iterator from the sort-merger
				return sortMerger;
			} catch (MemoryAllocationException mae) {
				throw new RuntimeException(
					"MemoryManager is not able to provide the required amount of memory for ReduceTask", mae);
			} catch (IOException ioe) {
				throw new RuntimeException("IOException caught when obtaining SortMerger for ReduceTask", ioe);
			}
		}
		default:
			throw new RuntimeException("Invalid local strategy provided for ReduceTask.");
		}

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
				int dop = getRuntimeConfiguration().getInteger(DEGREE_OF_PARALLELISM_KEY, -1);
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
