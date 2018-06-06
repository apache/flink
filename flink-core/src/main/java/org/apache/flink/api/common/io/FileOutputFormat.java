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

package org.apache.flink.api.common.io;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.flink.annotation.Public;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.FileSystem.WriteMode;

/**
 * The abstract base class for all Rich output formats that are file based. Contains the logic to
 * open/close the target
 * file streams.
 */
@Public
public abstract class FileOutputFormat<IT> extends RichOutputFormat<IT> implements InitializeOnMaster, CleanupWhenUnsuccessful {
	
	private static final long serialVersionUID = 1L;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Behavior for creating output directories. 
	 */
	public static enum OutputDirectoryMode {
		
		/** A directory is always created, regardless of number of write tasks. */
		ALWAYS,	
		
		/** A directory is only created for parallel output tasks, i.e., number of output tasks &gt; 1.
		 * If number of output tasks = 1, the output is written to a single file. */
		PARONLY
	}
	
	// --------------------------------------------------------------------------------------------

	private static WriteMode DEFAULT_WRITE_MODE;
	
	private static OutputDirectoryMode DEFAULT_OUTPUT_DIRECTORY_MODE;

	static {
		initDefaultsFromConfiguration(GlobalConfiguration.loadConfiguration());
	}

	/**
	 * Initialize defaults for output format. Needs to be a static method because it is configured for local
	 * cluster execution, see LocalFlinkMiniCluster.
	 * @param configuration The configuration to load defaults from
	 */
	public static void initDefaultsFromConfiguration(Configuration configuration) {
		final boolean overwrite = configuration.getBoolean(CoreOptions.FILESYTEM_DEFAULT_OVERRIDE);
	
		DEFAULT_WRITE_MODE = overwrite ? WriteMode.OVERWRITE : WriteMode.NO_OVERWRITE;
		
		final boolean alwaysCreateDirectory = configuration.getBoolean(CoreOptions.FILESYSTEM_OUTPUT_ALWAYS_CREATE_DIRECTORY);
	
		DEFAULT_OUTPUT_DIRECTORY_MODE = alwaysCreateDirectory ? OutputDirectoryMode.ALWAYS : OutputDirectoryMode.PARONLY;
	}

	// --------------------------------------------------------------------------------------------	
	
	/**
	 * The LOG for logging messages in this class.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(FileOutputFormat.class);
	
	/**
	 * The key under which the name of the target path is stored in the configuration. 
	 */
	public static final String FILE_PARAMETER_KEY = "flink.output.file";

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

	// --------------------------------------------------------------------------------------------
	
	/** The stream to which the data is written; */
	protected transient FSDataOutputStream stream;
	
	/** The path that is actually written to (may a a file in a the directory defined by {@code outputFilePath} ) */
	private transient Path actualFilePath;
	
	/** Flag indicating whether this format actually created a file, which should be removed on cleanup. */
	private transient boolean fileCreated;

	// --------------------------------------------------------------------------------------------

	public FileOutputFormat() {}

	public FileOutputFormat(Path outputPath) {
		this.outputFilePath = outputPath;
	}
	
	public void setOutputFilePath(Path path) {
		if (path == null) {
			throw new IllegalArgumentException("Output file path may not be null.");
		}
		
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
	}

	
	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		if (taskNumber < 0 || numTasks < 1) {
			throw new IllegalArgumentException("TaskNumber: " + taskNumber + ", numTasks: " + numTasks);
		}
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Opening stream for output (" + (taskNumber+1) + "/" + numTasks + "). WriteMode=" + writeMode +
					", OutputDirectoryMode=" + outputDirectoryMode);
		}
		
		Path p = this.outputFilePath;
		if (p == null) {
			throw new IOException("The file path is null.");
		}
		
		final FileSystem fs = p.getFileSystem();

		// if this is a local file system, we need to initialize the local output directory here
		if (!fs.isDistributedFS()) {
			
			if (numTasks == 1 && outputDirectoryMode == OutputDirectoryMode.PARONLY) {
				// output should go to a single file
				
				// prepare local output path. checks for write mode and removes existing files in case of OVERWRITE mode
				if(!fs.initOutPathLocalFS(p, writeMode, false)) {
					// output preparation failed! Cancel task.
					throw new IOException("Output path '" + p.toString() + "' could not be initialized. Canceling task...");
				}
			}
			else {
				// numTasks > 1 || outDirMode == OutputDirectoryMode.ALWAYS
				
				if(!fs.initOutPathLocalFS(p, writeMode, true)) {
					// output preparation failed! Cancel task.
					throw new IOException("Output directory '" + p.toString() + "' could not be created. Canceling task...");
				}
			}
		}



		// Suffix the path with the parallel instance index, if needed
		this.actualFilePath = (numTasks > 1 || outputDirectoryMode == OutputDirectoryMode.ALWAYS) ? p.suffix("/" + getDirectoryFileName(taskNumber)) : p;

		// create output file
		this.stream = fs.create(this.actualFilePath, writeMode);
		
		// at this point, the file creation must have succeeded, or an exception has been thrown
		this.fileCreated = true;
	}

	protected String getDirectoryFileName(int taskNumber) {
		return Integer.toString(taskNumber + 1);
	}

	@Override
	public void close() throws IOException {
		final FSDataOutputStream s = this.stream;
		if (s != null) {
			this.stream = null;
			s.close();
		}
	}
	
	/**
	 * Initialization of the distributed file system if it is used.
	 *
	 * @param parallelism The task parallelism.
	 */
	@Override
	public void initializeGlobal(int parallelism) throws IOException {
		final Path path = getOutputFilePath();
		final FileSystem fs = path.getFileSystem();
		
		// only distributed file systems can be initialized at start-up time.
		if (fs.isDistributedFS()) {
			
			final WriteMode writeMode = getWriteMode();
			final OutputDirectoryMode outDirMode = getOutputDirectoryMode();

			if (parallelism == 1 && outDirMode == OutputDirectoryMode.PARONLY) {
				// output is not written in parallel and should be written to a single file.
				// prepare distributed output path
				if(!fs.initOutPathDistFS(path, writeMode, false)) {
					// output preparation failed! Cancel task.
					throw new IOException("Output path could not be initialized.");
				}

			} else {
				// output should be written to a directory

				// only distributed file systems can be initialized at start-up time.
				if(!fs.initOutPathDistFS(path, writeMode, true)) {
					throw new IOException("Output directory could not be created.");
				}
			}
		}
	}
	
	@Override
	public void tryCleanupOnError() {
		if (this.fileCreated) {
			this.fileCreated = false;
			
			try {
				close();
			} catch (IOException e) {
				LOG.error("Could not properly close FileOutputFormat.", e);
			}

			try {
				FileSystem.get(this.actualFilePath.toUri()).delete(actualFilePath, false);
			} catch (FileNotFoundException e) {
				// ignore, may not be visible yet or may be already removed
			} catch (Throwable t) {
				LOG.error("Could not remove the incomplete file " + actualFilePath + '.', t);
			}
		}
	}
}
