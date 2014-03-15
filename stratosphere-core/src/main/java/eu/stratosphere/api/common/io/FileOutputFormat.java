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

package eu.stratosphere.api.common.io;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.core.fs.FSDataOutputStream;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.FileSystem.WriteMode;
import eu.stratosphere.core.fs.Path;

/**
 * The abstract base class for all output formats that are file based. Contains the logic to open/close the target
 * file streams.
 */
public abstract class FileOutputFormat<IT> implements OutputFormat<IT> {
	private static final long serialVersionUID = 1L;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Defines the behavior for creating output directories. 
	 *
	 */
	public static enum OutputDirectoryMode {
		
		/** A directory is always created, regardless of number of write tasks. */
		ALWAYS,	
		
		/** A directory is only created for parallel output tasks, i.e., number of output tasks > 1.
		 * If number of output tasks = 1, the output is written to a single file. */
		PARONLY
	}
	
	// --------------------------------------------------------------------------------------------

	private static WriteMode DEFAULT_WRITE_MODE;
	
	private static  OutputDirectoryMode DEFAULT_OUTPUT_DIRECTORY_MODE;
	
	
	private static final void initDefaultsFromConfiguration() {
		final boolean overwrite = GlobalConfiguration.getBoolean(ConfigConstants.FILESYSTEM_DEFAULT_OVERWRITE_KEY,
			ConfigConstants.DEFAULT_FILESYSTEM_OVERWRITE);
	
		DEFAULT_WRITE_MODE = overwrite ? WriteMode.OVERWRITE : WriteMode.NO_OVERWRITE;
		
		final boolean alwaysCreateDirectory = GlobalConfiguration.getBoolean(ConfigConstants.FILESYSTEM_OUTPUT_ALWAYS_CREATE_DIRECTORY_KEY,
			ConfigConstants.DEFAULT_FILESYSTEM_ALWAYS_CREATE_DIRECTORY);
	
		DEFAULT_OUTPUT_DIRECTORY_MODE = alwaysCreateDirectory ? OutputDirectoryMode.ALWAYS : OutputDirectoryMode.PARONLY;
	}
	
	static {
		initDefaultsFromConfiguration();
	}
	
	// --------------------------------------------------------------------------------------------	
	
	/**
	 * The LOG for logging messages in this class.
	 */
	private static final Log LOG = LogFactory.getLog(FileOutputFormat.class);
	
	/**
	 * The key under which the name of the target path is stored in the configuration. 
	 */
	public static final String FILE_PARAMETER_KEY = "stratosphere.output.file";
	
	/**
	 * The path of the file to be written.
	 */
	protected Path outputFilePath;
	
	/**
	 * The write mode of the output.	
	 */
	private WriteMode writeMode;
	
	/**
	 * The output directory mode
	 */
	private OutputDirectoryMode outputDirectoryMode;
	
	/**
	 * Stream opening timeout.
	 */
	private long openTimeout = -1;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * The stream to which the data is written;
	 */
	protected transient FSDataOutputStream stream;

	// --------------------------------------------------------------------------------------------
	
	public FileOutputFormat() {}
	
	public FileOutputFormat(Path outputPath) {
		this.outputFilePath = outputPath;
	}
	
	
	public void setOutputFilePath(Path path) {
		if (path == null)
			throw new IllegalArgumentException("Output file path may not be null.");
		
		this.outputFilePath = path;
	}
	
	public Path getOutputFilePath() {
		return this.outputFilePath;
	}

	
	public void setWriteMode(WriteMode mode) {
		if (mode == null) {
			throw new NullPointerException();
		}
		
		this.writeMode = mode;
	}
	
	public WriteMode getWriteMode() {
		return this.writeMode;
	}

	
	public void setOutputDirectoryMode(OutputDirectoryMode mode) {
		if (mode == null) {
			throw new NullPointerException();
		}
		
		this.outputDirectoryMode = mode;
	}
	
	public OutputDirectoryMode getOutputDirectoryMode() {
		return this.outputDirectoryMode;
	}
	
	
	public void setOpenTimeout(long timeout) {
		if (timeout < 0) {
			throw new IllegalArgumentException("The timeout must be a nonnegative numer of milliseconds (zero for infinite).");
		}
		
		this.openTimeout = (timeout == 0) ? Long.MAX_VALUE : timeout;
	}
	
	public long getOpenTimeout() {
		return this.openTimeout;
	}
	
	// ----------------------------------------------------------------

	@Override
	public void configure(Configuration parameters) {
		// get the output file path, if it was not yet set
		if (this.outputFilePath == null) {
			// get the file parameter
			String filePath = parameters.getString(FILE_PARAMETER_KEY, null);
			if (filePath == null) {
				throw new IllegalArgumentException("The output path has been specified neither via constructor/setters" +
						", nor via the Configuration.");
			}
			
			try {
				this.outputFilePath = new Path(filePath);
			}
			catch (RuntimeException rex) {
				throw new RuntimeException("Could not create a valid URI from the given file path name: " + rex.getMessage()); 
			}
		}
		
		// check if have not been set and use the defaults in that case
		if (this.writeMode == null) {
			this.writeMode = DEFAULT_WRITE_MODE;
		}
		
		if (this.outputDirectoryMode == null) {
			this.outputDirectoryMode = DEFAULT_OUTPUT_DIRECTORY_MODE;
		}
		
		if (this.openTimeout == -1) {
			this.openTimeout = FileInputFormat.getDefaultOpeningTimeout();
		}
	}

	
	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		
		if (LOG.isDebugEnabled())
			LOG.debug("Openint stream for output (" + (taskNumber+1) + "/" + numTasks + "). WriteMode=" + writeMode +
					", OutputDirectoryMode=" + outputDirectoryMode + ", timeout=" + openTimeout);
		
		// obtain FSDataOutputStream asynchronously, since HDFS client is vulnerable to InterruptedExceptions
		OutputPathOpenThread opot = new OutputPathOpenThread(this, (taskNumber + 1), numTasks);
		opot.start();
		
		try {
			// get FSDataOutputStream
			this.stream = opot.waitForCompletion();
		}
		catch (Exception e) {
			throw new RuntimeException("Stream to output file could not be opened: " + e.getMessage(), e);
		}
	}


	@Override
	public void close() throws IOException {
		final FSDataOutputStream s = this.stream;
		if (s != null) {
			this.stream = null;
			s.close();
		}
	}
	
	// ============================================================================================
	
	private static final class OutputPathOpenThread extends Thread {
		
		private final Path path;
		
		private final int taskIndex;
		
		private final int numTasks;
		
		private final WriteMode writeMode;
		
		private final OutputDirectoryMode outDirMode;
		
		private final long timeoutMillies;
		
		private volatile FSDataOutputStream fdos;

		private volatile Throwable error;
		
		private volatile boolean aborted;

		
		public OutputPathOpenThread(FileOutputFormat<?> fof, int taskIndex, int numTasks) {
			this.path = fof.getOutputFilePath();
			this.writeMode = fof.getWriteMode();
			this.outDirMode = fof.getOutputDirectoryMode();
			this.timeoutMillies = fof.getOpenTimeout();
			this.taskIndex = taskIndex;
			this.numTasks = numTasks;
		}

		@Override
		public void run() {

			try {
				Path p = this.path;
				final FileSystem fs = p.getFileSystem();

				// initialize output path. 
				if(this.numTasks == 1 && outDirMode == OutputDirectoryMode.PARONLY) {
					// output is not written in parallel and should go to a single file
					
					if(!fs.isDistributedFS()) {
						// prepare local output path
						// checks for write mode and removes existing files in case of OVERWRITE mode
						if(!fs.initOutPathLocalFS(p, writeMode, false)) {
							// output preparation failed! Cancel task.
							throw new IOException("Output path could not be initialized. Canceling task.");
						}
					}
					
				} else if(this.numTasks > 1 || outDirMode == OutputDirectoryMode.ALWAYS) {
					// output is written in parallel into a directory or should always be written to a directory
					
					if(!fs.isDistributedFS()) {
						// File system is not distributed.
						// We need to prepare the output path on each executing node.
						if(!fs.initOutPathLocalFS(p, writeMode, true)) {
							// output preparation failed! Cancel task.
							throw new IOException("Output directory could not be created. Canceling task.");
						}
					}
					
					// Suffix the path with the parallel instance index
					p = p.suffix("/" + this.taskIndex);
					
				} else {
					// invalid number of subtasks (<= 0)
					throw new IllegalArgumentException("Invalid number of subtasks. Canceling task.");
				}
					
				// create output file
				switch(writeMode) {
				case NO_OVERWRITE: 
					this.fdos = fs.create(p, false);
					break;
				case OVERWRITE:
					this.fdos = fs.create(p, true);
					break;
				default:
					throw new IllegalArgumentException("Invalid write mode: "+writeMode);
				}
				
				// check for canceling and close the stream in that case, because no one will obtain it
				if (this.aborted) {
					final FSDataOutputStream f = this.fdos;
					this.fdos = null;
					f.close();
				}
			}
			catch (Throwable t) {
				this.error = t;
			}
		}
		
		public FSDataOutputStream waitForCompletion() throws Exception {
			final long start = System.currentTimeMillis();
			long remaining = this.timeoutMillies;
			
			do {
				try {
					this.join(remaining);
				} catch (InterruptedException iex) {
					// we were canceled, so abort the procedure
					abortWait();
					throw iex;
				}
			}
			while (this.error == null && this.fdos == null &&
					(remaining = this.timeoutMillies + start - System.currentTimeMillis()) > 0);
			
			if (this.error != null) {
				throw new IOException("Opening the file output stream failed" +
					(this.error.getMessage() == null ? "." : ": " + this.error.getMessage()), this.error);
			}
			
			if (this.fdos != null) {
				return this.fdos;
			} else {
				// double-check that the stream has not been set by now. we don't know here whether
				// a) the opener thread recognized the canceling and closed the stream
				// b) the flag was set such that the stream did not see it and we have a valid stream
				// In any case, close the stream and throw an exception.
				abortWait();
				
				final boolean stillAlive = this.isAlive();
				final StringBuilder bld = new StringBuilder(256);
				for (StackTraceElement e : this.getStackTrace()) {
					bld.append("\tat ").append(e.toString()).append('\n');
				}
				throw new IOException("Output opening request timed out. Opener was " + (stillAlive ? "" : "NOT ") + 
					" alive. Stack:\n" + bld.toString());
			}
		}
		
		/**
		 * Double checked procedure setting the abort flag and closing the stream.
		 */
		private final void abortWait() {
			this.aborted = true;
			final FSDataOutputStream outStream = this.fdos;
			this.fdos = null;
			if (outStream != null) {
				try {
					outStream.close();
				} catch (Throwable t) {}
			}
		}
	}
	
	// ============================================================================================
	
	/**
	 * Creates a configuration builder that can be used to set the input format's parameters to the config in a fluent
	 * fashion.
	 * 
	 * @return A config builder for setting parameters.
	 */
	public static ConfigBuilder configureFileFormat(FileDataSink target) {
		return new ConfigBuilder(target.getParameters());
	}
	
	/**
	 * A builder used to set parameters to the output format's configuration in a fluent way.
	 */
	public static abstract class AbstractConfigBuilder<T> {
		
		/**
		 * The configuration into which the parameters will be written.
		 */
		protected final Configuration config;
		
		// --------------------------------------------------------------------
		
		/**
		 * Creates a new builder for the given configuration.
		 * 
		 * @param targetConfig The configuration into which the parameters will be written.
		 */
		protected AbstractConfigBuilder(Configuration targetConfig) {
			this.config = targetConfig;
		}
	}
	
	/**
	 * A builder used to set parameters to the input format's configuration in a fluent way.
	 */
	public static class ConfigBuilder extends AbstractConfigBuilder<ConfigBuilder> {
		
		/**
		 * Creates a new builder for the given configuration.
		 * 
		 * @param targetConfig The configuration into which the parameters will be written.
		 */
		protected ConfigBuilder(Configuration targetConfig) {
			super(targetConfig);
		}
	}
}