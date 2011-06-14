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
import java.util.Comparator;
import java.util.Iterator;

import javax.swing.SortOrder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.fs.FSDataOutputStream;
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
import eu.stratosphere.nephele.template.AbstractFileOutputTask;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.io.OutputFormat;
import eu.stratosphere.pact.common.stub.Stub;
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
 * The task writes data to files and uses an OutputFormat to serialize KeyValuePairs to a binary data stream.
 * Currently, the distributed Hadoop Filesystem (HDFS) is the only supported data storage.
 * 
 * @see eu.stratosphere.pact.common.io.OutputFormat
 * @author Fabian Hueske
 */
@SuppressWarnings( { "unchecked", "rawtypes" })
public class DataSinkTask extends AbstractFileOutputTask {

	// Obtain DataSinkTask Logger
	private static final Log LOG = LogFactory.getLog(DataSinkTask.class);
	
	public static final String SORT_ORDER = "sink.sort.order";

	// input reader
	private RecordReader<KeyValuePair<Key, Value>> reader;

	// OutputFormat instance
	private OutputFormat format;

	// task configuration
	private DataSinkConfig config;

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
	public void registerInputOutput() {
		LOG.debug("Start registering input and output: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		// initialize OutputFormat
		initOutputFormat();
		// initialize input reader
		initInputReader();
		
		// set up memory and I/O parameters
		this.availableMemory = config.getMemorySize();
		this.maxFileHandles = config.getNumFilehandles();
		this.spillThreshold = config.getSortSpillingTreshold();

		LOG.debug("Finished registering input and output: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {
		
		LOG.info("Start PACT code: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		final Path path = getFileOutputPath();
		final OutputFormat format = this.format;

		// obtain FSDataOutputStream asynchronously, since HDFS client can not handle InterruptedExceptions
		OutputPathOpenThread opot = new OutputPathOpenThread(path, 
			this.getEnvironment().getIndexInSubtaskGroup() + 1, 10000);
		opot.start();
		
		FSDataOutputStream fdos = null;
		
		// obtain grouped iterator
		CloseableInputProvider<KeyValuePair<Key, Value>> sortedInputProvider = null;
		
		try {
			sortedInputProvider = obtainInput();
			Iterator<KeyValuePair<Key, Value>> iter = sortedInputProvider.getIterator();
			
			LOG.debug("Iterator obtained: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
			
			// get FSDataOutputStream
			fdos = opot.getFSDataOutputStream();

			// check if task has been canceled
			if (this.taskCanceled) {
				return;
			}

			LOG.debug("Start writing output to " + path.toString() + " : " + this.getEnvironment().getTaskName()
				+ " (" + (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

			format.setOutput(fdos);
			format.open();

			while (!this.taskCanceled && iter.hasNext()) {
				KeyValuePair pair = iter.next();
				format.writePair(pair);
			}
		}
		catch (Exception ex) {
			// drop, if the task was canceled
			if (!this.taskCanceled) {
				LOG.error("Unexpected ERROR in PACT code: " + this.getEnvironment().getTaskName() + " ("
					+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
					+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
				throw ex;
			}
				
		}
		finally {
			if (sortedInputProvider != null) {
				sortedInputProvider.close();
			}
			
			if (this.format != null) {
				// close format
				try {
					this.format.close();
				}
				catch (Throwable t) {}
			}
				
			if (fdos != null) {
				// close file stream
				try {
					fdos.close();
				}
				catch (Throwable t) {}
			}
		}

		if (!this.taskCanceled) {
			LOG.debug("Finished writing output to " + path.toString() + " : " + this.getEnvironment().getTaskName()
				+ " (" + (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

			LOG.info("Finished PACT code: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
		} else {
			LOG.warn("PACT code cancelled: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInvokable#cancel()
	 */
	@Override
	public void cancel() throws Exception {
		this.taskCanceled = true;
		LOG.warn("Cancelling PACT code: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
	}

	/**
	 * Initializes the OutputFormat implementation and configuration.
	 * 
	 * @throws RuntimeException
	 *         Throws if instance of OutputFormat implementation can not be
	 *         obtained.
	 */
	private void initOutputFormat() throws RuntimeException {

		// obtain task configuration (including stub parameters)
		config = new DataSinkConfig(getRuntimeConfiguration());

		// obtain stub implementation class
		ClassLoader cl;
		try {
			cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			Class<? extends OutputFormat> formatClass = config.getStubClass(OutputFormat.class, cl);
			// obtain instance of stub implementation
			this.format = formatClass.newInstance();
			// configure stub implementation
			this.format.configure(this.config.getStubParameters());

		} catch (IOException ioe) {
			throw new RuntimeException("Library cache manager could not be instantiated.", ioe);
		} catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("OutputFormat implementation class was not found.", cnfe);
		} catch (InstantiationException ie) {
			throw new RuntimeException("OutputFormat implementation could not be instanciated.", ie);
		} catch (IllegalAccessException iae) {
			throw new RuntimeException("OutputFormat implementations nullary constructor is not accessible.", iae);
		}
	}

	/**
	 * Initializes the input reader of the DataSinkTask.
	 * 
	 * @throws RuntimeException
	 *         Thrown if no input ship strategy was provided.
	 */
	private void initInputReader() throws RuntimeException {

		// create RecordDeserializer
		RecordDeserializer<KeyValuePair<Key, Value>> deserializer = new KeyValuePairDeserializer(
			format.getOutKeyType(), format.getOutValueType());

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
		final Class<Key> keyClass = format.getOutKeyType();
		// obtain input value type
		final Class<Value> valueClass = format.getOutValueType();

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
			final Order sortOrder = Order.valueOf(config.getStubParameters().getString(SORT_ORDER, ""));
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
				// instantiate a sort-merge
				SortMerger<Key, Value> sortMerger = new UnilateralSortMerger<Key, Value>(memoryManager, ioManager,
						this.availableMemory, this.maxFileHandles, keySerialization, valSerialization,
						keyComparator, reader, this, this.spillThreshold);
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

	public static class DataSinkConfig extends TaskConfig {

		private static final String FORMAT_CLASS = "formatClass";

		private static final String FILE_PATH = "outputPath";

		public DataSinkConfig(Configuration config) {
			super(config);
		}

		@Override
		public void setStubClass(Class<? extends Stub<?, ?>> formatClass) {
			config.setString(FORMAT_CLASS, formatClass.getName());
		}

		@Override
		public <T extends Stub<?, ?>> Class<? extends T> getStubClass(Class<T> formatClass, ClassLoader cl)
				throws ClassNotFoundException {
			String formatClassName = config.getString(FORMAT_CLASS, null);
			if (formatClassName == null) {
				throw new IllegalStateException("format class missing");
			}

			return Class.forName(formatClassName, true, cl).asSubclass(formatClass);
		}

		public void setFilePath(String filePath) {
			config.setString(FILE_PATH, filePath);
		}

		public String getFilePath() {
			return config.getString(FILE_PATH, null);
		}
	}

	/**
	 * Obtains a DataOutputStream in an thread that is not interrupted.
	 * The HDFS client is very sensitive to InterruptedExceptions.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 */
	public static class OutputPathOpenThread extends Thread {

		private final Object lock = new Object();
		
		private final Path path;
		
		private final long timeoutMillies;

		private final int taskIndex;

		private volatile FSDataOutputStream fdos;

		private volatile Exception exception;
		
		private volatile boolean canceled = false;
		

		public OutputPathOpenThread(Path path, int taskIndex, long timeoutMillies) {
			this.path = path;
			this.timeoutMillies = timeoutMillies;
			this.taskIndex = taskIndex;
		}

		@Override
		public void run() {
			
			try {
				final FileSystem fs = path.getFileSystem();
				Path p = this.path;
				
				if (fs.exists(this.path) && fs.getFileStatus(this.path).isDir()) {
					// write output in directory
					p = this.path.suffix("/" + this.taskIndex);
				}
				
				final FSDataOutputStream stream = fs.create(p, true);

				// create output file
				synchronized (this.lock) {
					this.lock.notifyAll();
					
					if (!this.canceled) {
						this.fdos = stream;
					}
					else {
						this.fdos = null;
						stream.close();
					}
				}
			}
			catch (Exception t) {
				synchronized (this.lock) {
					this.canceled = true;
					this.exception = t;
				}
			}
		}

		public FSDataOutputStream getFSDataOutputStream()
		throws Exception
		{
			long start = System.currentTimeMillis();
			long remaining = this.timeoutMillies;
			
			if (this.exception != null) {
				throw this.exception;
			}
			if (this.fdos != null) {
				return this.fdos;
			}
			
			synchronized (this.lock) {
				do {
					try {
						this.lock.wait(remaining);
					}
					catch (InterruptedException iex) {
						this.canceled = true;
						if (this.fdos != null) {
							try  {
								this.fdos.close();
							} catch (Throwable t) {}
						}
						throw new Exception("Output Path Opener was interrupted.");
					}
				}
				while (this.exception == null && this.fdos == null &&
						(remaining = this.timeoutMillies + start - System.currentTimeMillis()) > 0);
			
				if (this.exception != null) {
					if (this.fdos != null) {
						try  {
							this.fdos.close();
						} catch (Throwable t) {}
					}
					throw this.exception;
				}
				
				if (this.fdos != null) {
					return this.fdos;
				}
			}
			
			// try to forcefully shut this thread down
			throw new Exception("Output Path Opener timed out.");
		}
	}

}
