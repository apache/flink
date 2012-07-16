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

package eu.stratosphere.pact.common.io;


import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.generic.io.OutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;


/**
 * The abstract base class for all output formats that are file based. Contains the logic to open/close the target
 * file streams.
 */
public abstract class FileOutputFormat implements OutputFormat<PactRecord>
{
	/**
	 * The key under which the name of the target path is stored in the configuration. 
	 */
	public static final String FILE_PARAMETER_KEY = "pact.output.file";
	
	/**
	 * The path of the file to be written.
	 */
	protected Path outputFilePath;
	
	/**
	 * The stream to which the data is written;
	 */
	protected FSDataOutputStream stream;

	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordio.OutputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration parameters)
	{
		String filePath = parameters.getString(FILE_PARAMETER_KEY, null);
		if (filePath == null) {
			throw new IllegalArgumentException("Configuration file FileOutputFormat does not contain the file path.");
		}
		
		try {
			this.outputFilePath = new Path(filePath);
		}
		catch (RuntimeException rex) {
			throw new RuntimeException("Could not create a valid URI from the given file path name: " + rex.getMessage()); 
		}
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordio.OutputFormat#open()
	 */
	@Override
	public void open(int taskNumber) throws IOException
	{
		// obtain FSDataOutputStream asynchronously, since HDFS client can not handle InterruptedExceptions
		OutputPathOpenThread opot = new OutputPathOpenThread(this.outputFilePath, taskNumber, 10000);
		opot.start();
		
		try {
			// get FSDataOutputStream
			this.stream = opot.waitForCompletion();
		}
		catch (Exception e) {
			throw new RuntimeException("Stream to output file could not be opened: " + e.getMessage(), e);
		}
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordio.OutputFormat#close()
	 */
	@Override
	public void close() throws IOException
	{
		final FSDataOutputStream s = this.stream;
		if (s != null) {
			this.stream = null;
			s.close();
		}
	}
	
	// ============================================================================================
	
	private static final class OutputPathOpenThread extends Thread
	{
		private final Path path;
		
		private final int taskIndex;
		
		private final long timeoutMillies;
		
		private volatile FSDataOutputStream fdos;

		private volatile Throwable error;
		
		private volatile boolean aborted;

		
		public OutputPathOpenThread(Path path, int taskIndex, long timeoutMillies) {
			this.path = path;
			this.timeoutMillies = timeoutMillies;
			this.taskIndex = taskIndex;
		}

		@Override
		public void run()
		{
			try {
				Path p = this.path;
				final FileSystem fs = p.getFileSystem();
				
				// if the output is a directory, suffix the path with the parallel instance index
				if (fs.exists(p) && fs.getFileStatus(p).isDir()) {
					p = p.suffix("/" + this.taskIndex);
				}
				
				// remove the existing file before creating the output stream
				if (fs.exists(p)) {
					fs.delete(p, false);
				}
				
				this.fdos = fs.create(p, true);
				
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
		
		public FSDataOutputStream waitForCompletion() throws Exception
		{
			final long start = System.currentTimeMillis();
			long remaining = this.timeoutMillies;
			
			do {
				try {
					this.join(remaining);
				} catch (InterruptedException iex) {}
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
				this.aborted = true;
				// double-check that the stream has not been set by now. we don't know here whether
				// a) the opener thread recognized the canceling and closed the stream
				// b) the flag was set such that the stream did not see it and we have a valid stream
				// In any case, close the stream and throw an exception.
				final FSDataOutputStream outStream = this.fdos;
				this.fdos = null;
				if (outStream != null) {
					try {
						outStream.close();
					} catch (Throwable t) {}
				}
				throw new IOException("Opening request timed out.");
			}
		}
	}
}