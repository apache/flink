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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.PointwiseDistributionPattern;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractFileOutputTask;
import eu.stratosphere.pact.common.io.OutputFormat;
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairDeserializer;
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

	// input reader
	private RecordReader<KeyValuePair<Key, Value>> reader;

	// OutputFormat instance
	private OutputFormat format;

	// task configuration
	private DataSinkConfig config;

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

		final Path path = getFileOutputPath();
		final RecordReader<KeyValuePair<Key, Value>> reader = this.reader;
		final OutputFormat format = this.format;

		// obtain FSDataOutputStream asynchronously, since HDFS client can not handle InterruptedExceptions
		OutputPathOpenThread opot = new OutputPathOpenThread(path, 
			this.getEnvironment().getIndexInSubtaskGroup() + 1, 10000);
		opot.start();
		
		FSDataOutputStream fdos = null;
		
		try {
			// get FSDataOutputStream
			fdos = opot.getFSDataOutputStream();

			// check if task has been canceled
			if (this.taskCanceled) {
				return;
			}

			if (LOG.isDebugEnabled())
				LOG.debug(getLogString("Start writing output to " + path.toString()));

			format.setOutput(fdos);
			format.open();

			while (!this.taskCanceled && reader.hasNext()) {
				KeyValuePair pair = reader.next();
				format.writePair(pair);
			}
		}
		catch (Exception ex) {
			// drop, if the task was canceled
			if (!this.taskCanceled) {
				if (LOG.isErrorEnabled())
					LOG.error(getLogString("Unexpected ERROR in PACT code"));
				throw ex;
			}
				
		}
		finally {
			
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
			if (LOG.isDebugEnabled())
				LOG.debug(getLogString("Finished writing output to " + path.toString()));

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
	private void initInputReader()
	{
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
		default:
			throw new RuntimeException("No valid input ship strategy provided for DataSinkTask.");
		}

		// create reader
		// map has only one input, so we create one reader (id=0).
		this.reader = new RecordReader<KeyValuePair<Key, Value>>(this, deserializer, dp);

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
		bld.append(' ').append('"');
		bld.append(this.getEnvironment().getIndexInSubtaskGroup() + 1);
		bld.append('/');
		bld.append(this.getEnvironment().getCurrentNumberOfSubtasks());
		bld.append(')');
		return bld.toString();
	}
}
